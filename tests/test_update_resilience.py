"""Regression tests for telemetry starvation, stale data, and command races."""

import asyncio
from collections.abc import Callable
import threading
from unittest.mock import MagicMock, patch

from freezegun.api import FrozenDateTimeFactory
from libdeye.client import DeyeDevice
from libdeye.cloud_api import DeyeCloudApiCannotConnectError
from libdeye.device_state import DeyeDeviceState
from libdeye.mqtt_client import DeyeFogMqttClient
import pytest
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.deye_dehumidifier import _async_disconnect_client
from custom_components.deye_dehumidifier.const import DOMAIN, STATE_DATA_MAX_AGE
from custom_components.deye_dehumidifier.data_coordinator import (
    DeyeDataUpdateCoordinator,
)
from custom_components.deye_dehumidifier.humidifier import DeyeDehumidifier
from custom_components.deye_dehumidifier.sensor import DeyeHumiditySensor
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from tests.helpers import MOCK_CONFIG, MOCK_DEVICE_INFO
from tests.test_state_recovery import _advance


async def test_online_notifications_do_not_postpone_http_polls(
    hass: HomeAssistant,
    fog_coordinator: tuple[DeyeDataUpdateCoordinator, MagicMock],
    freezer: FrozenDateTimeFactory,
) -> None:
    """Availability traffic cannot postpone the periodic telemetry refresh."""
    coordinator, api = fog_coordinator
    remove = coordinator.async_add_listener(lambda: None)
    try:
        for _ in range(12):
            await _advance(hass, freezer, 10)
            coordinator.update_device_availability(True)
        assert api.get_fog_platform_device_properties.await_count >= 4
        assert coordinator.data.available
    finally:
        remove()


async def test_frequent_commands_keep_receiving_telemetry(
    hass: HomeAssistant,
    fog_coordinator: tuple[DeyeDataUpdateCoordinator, MagicMock],
    freezer: FrozenDateTimeFactory,
) -> None:
    """Repeated controls must not stop HTTP polls or freeze humidity."""
    coordinator, api = fog_coordinator
    entity = DeyeDehumidifier(coordinator, coordinator.device.info)
    try:
        for index in range(24):
            api.get_fog_platform_device_properties.return_value = {
                "CurrentEnvironmentalHumidity": 22 + index,
                "CurrentAmbientTemperature": 27,
            }
            await entity.async_set_humidity(45 + index % 2)
            await coordinator.async_refresh()
            assert coordinator.data.state.environment_humidity == 22 + index
            await _advance(hass, freezer, 5)
        assert api.get_fog_platform_device_properties.await_count == 25
    finally:
        entity._debounced_publish_command.async_shutdown()


async def test_command_protection_is_bounded_and_preserves_only_pending_fields(
    hass: HomeAssistant,
    fog_coordinator: tuple[DeyeDataUpdateCoordinator, MagicMock],
    freezer: FrozenDateTimeFactory,
) -> None:
    """Commands keep their target while telemetry and unrelated controls update."""
    coordinator, _ = fog_coordinator
    coordinator.data.state.target_humidity = 45
    coordinator.mute_state_update_for_a_while()
    await _advance(hass, freezer, 5)
    coordinator.mute_state_update_for_a_while()
    reported = coordinator.data.reported_state.copy()
    reported.environment_humidity = 63
    reported.power_switch = False
    coordinator.update_device_state(reported)
    assert coordinator.data.reported_state is reported
    assert coordinator.data.state.target_humidity == 45
    assert coordinator.data.state.environment_humidity == 63
    assert coordinator.data.state.power_switch is False
    await _advance(hass, freezer, 6)
    assert coordinator.state_update_muted is None


async def test_http_outage_expires_data_and_success_restores_it(
    hass: HomeAssistant,
    fog_coordinator: tuple[DeyeDataUpdateCoordinator, MagicMock],
    freezer: FrozenDateTimeFactory,
) -> None:
    """Grace allows brief outages; cloud online status cannot revive stale data."""
    coordinator, api = fog_coordinator
    sensor = DeyeHumiditySensor(coordinator, coordinator.device.info)
    last_received = coordinator.last_state_received
    api.get_fog_platform_device_properties.side_effect = DeyeCloudApiCannotConnectError
    await coordinator.async_refresh()
    await _advance(hass, freezer, 60)
    assert sensor.available
    assert not coordinator.state_data_stale
    coordinator.update_device_availability(True)
    await coordinator.async_refresh()
    await _advance(hass, freezer, STATE_DATA_MAX_AGE - 59)
    assert not sensor.available
    assert coordinator.state_data_stale
    assert sensor.native_value == 47
    assert coordinator.last_state_received == last_received
    coordinator.update_device_availability(True)
    assert not sensor.available
    api.get_fog_platform_device_properties.side_effect = None
    await coordinator.async_refresh()
    assert sensor.available
    assert not coordinator.state_data_stale


async def test_successful_command_does_not_make_old_telemetry_fresh(
    hass: HomeAssistant,
    fog_coordinator: tuple[DeyeDataUpdateCoordinator, MagicMock],
    freezer: FrozenDateTimeFactory,
) -> None:
    """An accepted setting is not evidence of fresh temperature or humidity."""
    coordinator, _ = fog_coordinator
    entity = DeyeDehumidifier(coordinator, coordinator.device.info)
    try:
        await _advance(hass, freezer, 90)
        await entity.async_set_humidity(45)
        await _advance(hass, freezer, 91)
        assert coordinator.state_data_stale
        assert not entity.available
    finally:
        entity._debounced_publish_command.async_shutdown()


@pytest.mark.parametrize("same_entity", [True, False])
async def test_pending_commands_are_sent_serially_without_losing_new_settings(
    hass: HomeAssistant,
    fog_coordinator: tuple[DeyeDataUpdateCoordinator, MagicMock],
    same_entity: bool,
) -> None:
    """A slow humidity command cannot swallow a newer power command."""
    coordinator, api = fog_coordinator
    coordinator.update_device_state(
        DeyeDeviceState(
            {
                "Power": 1,
                "SetHumidity": 55,
                "CurrentEnvironmentalHumidity": 47,
                "CurrentAmbientTemperature": 27,
            }
        )
    )
    first = DeyeDehumidifier(coordinator, coordinator.device.info)
    second = (
        first if same_entity else DeyeDehumidifier(coordinator, coordinator.device.info)
    )
    entered, finish = asyncio.Event(), asyncio.Event()
    active = 0
    maximum_active = 0

    async def slow_set(device_id: str, properties: dict[str, int]) -> None:
        nonlocal active, maximum_active
        active += 1
        maximum_active = max(maximum_active, active)
        try:
            entered.set()
            await finish.wait()
        finally:
            active -= 1

    api.set_fog_platform_device_properties.side_effect = slow_set
    task = hass.async_create_task(first.async_set_humidity(45))
    other_task: asyncio.Task[None] | None = None
    try:
        await entered.wait()
        other_task = hass.async_create_task(second.async_turn_off())
        await asyncio.sleep(0)
        assert coordinator.data.reported_state.power_switch is True
        reported = coordinator.data.reported_state.copy()
        reported.environment_humidity = 64
        coordinator.update_device_state(reported)
        assert coordinator.data.state.environment_humidity == 64
        finish.set()
        await task
        await other_task
        assert maximum_active == 1
        assert [
            call.args[1]
            for call in api.set_fog_platform_device_properties.call_args_list
        ] == [
            {"SetHumidity": 45},
            {"Power": 0},
        ]
        assert coordinator.data.reported_state.power_switch is False
        assert coordinator.data.state.environment_humidity == 64
    finally:
        finish.set()
        await task
        if other_task is not None:
            await other_task
        first._debounced_publish_command.async_shutdown()
        second._debounced_publish_command.async_shutdown()


async def test_timed_out_command_releases_pending_controls(
    fog_coordinator: tuple[DeyeDataUpdateCoordinator, MagicMock],
) -> None:
    """A stuck command has a deadline and cannot pin a failed desired setting."""
    coordinator, api = fog_coordinator
    entity = DeyeDehumidifier(coordinator, coordinator.device.info)

    async def never_finishes(*args: object) -> None:
        await asyncio.Event().wait()

    api.set_fog_platform_device_properties.side_effect = never_finishes
    try:
        with (
            patch(
                "custom_components.deye_dehumidifier.data_coordinator.COMMAND_TIMEOUT",
                0,
            ),
            pytest.raises(HomeAssistantError),
        ):
            await entity.async_set_humidity(45)
        assert (
            coordinator.data.state.target_humidity
            == coordinator.data.reported_state.target_humidity
        )
        assert not coordinator._pending_controls
    finally:
        entity._debounced_publish_command.async_shutdown()


async def test_failed_send_requeues_newer_request_after_debounce_timer_fired(
    hass: HomeAssistant,
    fog_coordinator: tuple[DeyeDataUpdateCoordinator, MagicMock],
    freezer: FrozenDateTimeFactory,
) -> None:
    """A failed slow send must not strand a newer request on the same entity."""
    coordinator, api = fog_coordinator
    coordinator.update_device_state(
        DeyeDeviceState(
            {
                "Power": 1,
                "SetHumidity": 55,
                "CurrentEnvironmentalHumidity": 47,
            }
        )
    )
    entity = DeyeDehumidifier(coordinator, coordinator.device.info)
    entered, finish = asyncio.Event(), asyncio.Event()
    calls = 0

    async def fail_first_send(device_id: str, properties: dict[str, int]) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            entered.set()
            await finish.wait()
            raise DeyeCloudApiCannotConnectError

    api.set_fog_platform_device_properties.side_effect = fail_first_send
    task = hass.async_create_background_task(
        entity.async_set_humidity(45), "Blocked humidity command"
    )
    try:
        await entered.wait()
        await entity.async_turn_off()
        await _advance(hass, freezer, 3)
        finish.set()
        with pytest.raises(HomeAssistantError):
            await task
        await _advance(hass, freezer, 3)
        assert calls == 2
        assert api.set_fog_platform_device_properties.call_args.args[1] == {"Power": 0}
        assert not coordinator.has_pending_commands
        assert coordinator.data.reported_state.power_switch is False
    finally:
        finish.set()
        entity._debounced_publish_command.async_shutdown()


async def test_cancelled_command_releases_pending_controls(
    hass: HomeAssistant,
    fog_coordinator: tuple[DeyeDataUpdateCoordinator, MagicMock],
) -> None:
    """Cancelling a service call must not pin an unsent desired value."""
    coordinator, api = fog_coordinator
    entity = DeyeDehumidifier(coordinator, coordinator.device.info)
    entered = asyncio.Event()

    async def never_finishes(*args: object) -> None:
        entered.set()
        await asyncio.Event().wait()

    api.set_fog_platform_device_properties.side_effect = never_finishes
    task = hass.async_create_task(entity.async_set_humidity(45))
    try:
        await entered.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert not coordinator._pending_controls
        api.set_fog_platform_device_properties.side_effect = None
        await entity.async_set_humidity(50)
        await entity._publish_command()
        assert coordinator.data.reported_state.target_humidity == 50
    finally:
        entity._debounced_publish_command.async_shutdown()


async def test_late_callbacks_cannot_restart_timers_after_shutdown(
    fog_coordinator: tuple[DeyeDataUpdateCoordinator, MagicMock],
) -> None:
    """Queued MQTT callbacks must not revive a coordinator after unload."""
    coordinator, _ = fog_coordinator
    received = coordinator.last_state_received
    await coordinator.async_shutdown()
    coordinator.update_device_state(coordinator.data.reported_state.copy())
    coordinator.update_device_availability(True)
    coordinator.mute_state_update_for_a_while()
    assert coordinator.last_state_received == received
    assert coordinator._stale_unsub is None
    assert coordinator.state_update_muted is None


async def test_slow_http_response_cannot_replace_newer_mqtt_state(
    hass: HomeAssistant,
    fog_coordinator: tuple[DeyeDataUpdateCoordinator, MagicMock],
) -> None:
    """Keep telemetry received after an HTTP request began, in both wrappers."""
    coordinator, api = fog_coordinator
    entered, finish = asyncio.Event(), asyncio.Event()

    async def old_properties(device_id: str) -> dict[str, int]:
        entered.set()
        await finish.wait()
        return {"CurrentEnvironmentalHumidity": 21}

    api.get_fog_platform_device_properties.side_effect = old_properties
    task = hass.async_create_task(coordinator.async_refresh())
    try:
        await entered.wait()
        mqtt = coordinator.device.client._mqtt_by_type[DeyeFogMqttClient]
        payload = {
            "device_id": coordinator.device.device_id,
            "biz_code": "device_data",
            "data": {
                "message_type": "thing_property",
                "properties": {"CurrentEnvironmentalHumidity": 64},
            },
        }
        for handler in mqtt._subscribers[""].copy():
            handler(payload)
        received = coordinator.last_state_received
        assert coordinator.data.state.environment_humidity == 64
        finish.set()
        await task
        assert coordinator.data.reported_state.environment_humidity == 64
        assert coordinator.data.state.environment_humidity == 64
        assert coordinator.device.reported_state.environment_humidity == 64
        assert coordinator.device.state.environment_humidity == 64
        assert coordinator.last_state_received == received
    finally:
        finish.set()
        await task


async def test_shutdown_cancels_initial_connection_before_it_can_subscribe(
    hass: HomeAssistant,
) -> None:
    """A slow credential request cannot start an orphan MQTT client after unload."""
    entered, finish = asyncio.Event(), asyncio.Event()
    device = DeyeDevice(MagicMock(), MOCK_DEVICE_INFO)
    entry = MockConfigEntry(domain=DOMAIN, data=MOCK_CONFIG, version=2)
    entry.add_to_hass(hass)
    coordinator = DeyeDataUpdateCoordinator(hass, entry, device)

    async def slow_connect() -> None:
        entered.set()
        await finish.wait()

    with (
        patch.object(device, "ensure_connected", side_effect=slow_connect),
        patch.object(device, "subscribe") as subscribe,
    ):
        task = hass.async_create_task(coordinator._async_setup())
        await entered.wait()
        await coordinator.async_shutdown()
        finish.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        subscribe.assert_not_called()
        assert not coordinator._active_requests
        assert coordinator._stale_unsub is None


async def test_shutdown_cancels_active_and_queued_commands(
    hass: HomeAssistant,
    fog_coordinator: tuple[DeyeDataUpdateCoordinator, MagicMock],
) -> None:
    """No queued command can use the old pooled client while it is disconnecting."""
    coordinator, api = fog_coordinator
    first = DeyeDehumidifier(coordinator, coordinator.device.info)
    second = DeyeDehumidifier(coordinator, coordinator.device.info)
    entered = asyncio.Event()

    async def never_finishes(*args: object) -> None:
        entered.set()
        await asyncio.Event().wait()

    api.set_fog_platform_device_properties.side_effect = never_finishes
    running = hass.async_create_task(first.async_set_humidity(45))
    await entered.wait()
    waiting = hass.async_create_task(second.async_turn_off())
    try:
        await coordinator.async_shutdown()
        for task in (running, waiting):
            with pytest.raises(asyncio.CancelledError):
                await task
        assert not coordinator._active_requests
        assert not coordinator.has_pending_commands
        api.set_fog_platform_device_properties.assert_awaited_once()
    finally:
        first._debounced_publish_command.async_shutdown()
        second._debounced_publish_command.async_shutdown()


async def _disconnect_with_blocked_worker(
    hass: HomeAssistant, check: Callable[[asyncio.Task[None]], None]
) -> None:
    """Bound the test's worker lifetime even if the event loop regresses."""
    release = threading.Event()
    client = MagicMock()
    client.disconnect = release.wait
    fallback = threading.Timer(2, release.set)
    fallback.start()
    task = hass.async_create_task(_async_disconnect_client(hass, client))
    try:
        await asyncio.sleep(0.02)
        check(task)
    finally:
        release.set()
        await task
        await hass.async_block_till_done()
        fallback.cancel()
        fallback.join()


async def test_disconnect_keeps_event_loop_responsive(hass: HomeAssistant) -> None:
    """A blocked MQTT thread must not prevent event loop timers from running."""

    def check(task: asyncio.Task[None]) -> None:
        assert not task.done()

    await _disconnect_with_blocked_worker(hass, check)


async def test_disconnect_wait_is_bounded(hass: HomeAssistant) -> None:
    """Unload can finish while an old MQTT client cleans up in a worker."""

    def check(task: asyncio.Task[None]) -> None:
        assert task.done()

    with patch("custom_components.deye_dehumidifier.MQTT_DISCONNECT_TIMEOUT", 0):
        await _disconnect_with_blocked_worker(hass, check)


async def test_late_disconnect_failure_is_reported(
    hass: HomeAssistant, caplog: pytest.LogCaptureFixture
) -> None:
    """A timed-out worker failure remains observable and is retrieved."""
    result: asyncio.Future[None] = hass.loop.create_future()
    with (
        patch.object(hass, "async_add_executor_job", return_value=result),
        patch("custom_components.deye_dehumidifier.MQTT_DISCONNECT_TIMEOUT", 0),
    ):
        await _async_disconnect_client(hass, MagicMock())
    result.set_exception(RuntimeError("worker cleanup failed"))
    await asyncio.sleep(0)
    assert "Deye MQTT background cleanup failed: worker cleanup failed" in caplog.text
