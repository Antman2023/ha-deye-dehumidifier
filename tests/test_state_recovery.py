"""Tests for online devices whose asynchronous state replies stop arriving."""

import asyncio
from collections.abc import AsyncGenerator
from datetime import timedelta
from typing import cast
from unittest.mock import MagicMock, patch

from freezegun.api import FrozenDateTimeFactory
from libdeye.client import DeyeClient, DeyeDevice
from libdeye.cloud_api import DeyeCloudApi, DeyeDeviceTransport, DeyeIotPlatform
from libdeye.mqtt_client import mqtt_client_type_for_device
import pytest
from pytest_homeassistant_custom_component.common import (
    MockConfigEntry,
    async_fire_time_changed,
)

from custom_components.deye_dehumidifier.const import (
    DATA_STATE_RECOVERY,
    DOMAIN,
    STATE_QUERY_FAILURES_BEFORE_RECOVERY,
    STATE_QUERY_TIMEOUT,
    STATE_RECOVERY_COOLDOWN,
    STATE_RECOVERY_DELAY,
)
from custom_components.deye_dehumidifier.data_coordinator import (
    DeyeDataUpdateCoordinator,
)
from custom_components.deye_dehumidifier.diagnostics import _coordinator_snapshot
from custom_components.deye_dehumidifier.sensor import DeyeHumiditySensor
from homeassistant.core import HomeAssistant
from homeassistant.util import dt as dt_util
from tests.helpers import MOCK_CONFIG, MOCK_DEVICE_INFO


@pytest.fixture(
    params=[DeyeDeviceTransport.CLASSIC, DeyeDeviceTransport.COMBO],
    ids=lambda transport: transport.name,
)
async def coordinator(
    hass: HomeAssistant,
    request: pytest.FixtureRequest,
    freezer: FrozenDateTimeFactory,
) -> AsyncGenerator[DeyeDataUpdateCoordinator]:
    """Use real libdeye refresh routing with only network transports mocked."""
    transport = request.param
    info = MOCK_DEVICE_INFO | {
        "platform": (
            DeyeIotPlatform.Fog
            if transport is DeyeDeviceTransport.FOG
            else DeyeIotPlatform.Classic
        ),
        "is_combo": transport is DeyeDeviceTransport.COMBO,
        "protocol_version": (
            "combo_V1.0" if transport is DeyeDeviceTransport.COMBO else "wifi_V2.4"
        ),
    }
    api = MagicMock(spec=DeyeCloudApi)
    client = DeyeClient(api)
    mqtt_type = mqtt_client_type_for_device(info)
    mqtt = MagicMock(spec=mqtt_type)
    client._mqtt_by_type[mqtt_type] = mqtt
    device = DeyeDevice(client, info)
    assert device.transport is transport
    entry = MockConfigEntry(domain=DOMAIN, data=MOCK_CONFIG, version=2)
    entry.add_to_hass(hass)
    result = DeyeDataUpdateCoordinator(hass, entry, device)
    await result._async_setup()
    await result.async_refresh()
    mqtt.publish_command.assert_awaited_once()
    yield result
    await result.async_shutdown()


async def _advance(
    hass: HomeAssistant, freezer: FrozenDateTimeFactory, seconds: float
) -> None:
    freezer.tick(timedelta(seconds=seconds))
    async_fire_time_changed(hass, dt_util.utcnow())
    await hass.async_block_till_done()


async def _stall(
    coordinator: DeyeDataUpdateCoordinator, freezer: FrozenDateTimeFactory
) -> None:
    for failure in range(STATE_QUERY_FAILURES_BEFORE_RECOVERY):
        if failure:
            await coordinator.async_refresh()
        await _advance(coordinator.hass, freezer, STATE_QUERY_TIMEOUT + 1)


async def test_missing_replies_reload_online_account(
    hass: HomeAssistant,
    coordinator: DeyeDataUpdateCoordinator,
    freezer: FrozenDateTimeFactory,
) -> None:
    """MQTT publish success cannot hide a stalled Classic/Combo subscriber."""
    sensor = DeyeHumiditySensor(coordinator, coordinator.device.info)
    with patch.object(hass.config_entries, "async_schedule_reload") as reload_entry:
        await _stall(coordinator, freezer)
        assert coordinator.state_updates_stalled
        assert not sensor.available
        assert coordinator.device.available  # The cloud still claims it is online.
        reload_entry.assert_not_called()
        await _advance(hass, freezer, STATE_RECOVERY_DELAY + 1)
    assert coordinator.config_entry is not None
    reload_entry.assert_called_once_with(coordinator.config_entry.entry_id)


@pytest.mark.parametrize("muted", [False, True])
async def test_real_reply_cancels_recovery_even_when_values_are_unchanged(
    hass: HomeAssistant,
    coordinator: DeyeDataUpdateCoordinator,
    freezer: FrozenDateTimeFactory,
    muted: bool,
) -> None:
    """Unchanged payloads and muted command echoes still prove reception works."""
    with patch.object(hass.config_entries, "async_schedule_reload") as reload_entry:
        await _stall(coordinator, freezer)
        desired = coordinator.data.state
        if muted:
            coordinator.state_update_muted = MagicMock()
        mqtt = cast(MagicMock, coordinator.device._mqtt)
        handler = mqtt.subscribe_state_change.call_args.args[2]
        handler(coordinator.data.reported_state.copy())
        await _advance(hass, freezer, STATE_RECOVERY_DELAY + 1)
    assert coordinator.data.available
    assert coordinator.state_query_failures == 0
    assert not coordinator.state_updates_stalled
    assert coordinator.last_state_received is not None
    if muted:
        assert coordinator.data.state is desired
    reload_entry.assert_not_called()


async def test_cloud_and_mqtt_online_flags_do_not_clear_stall(
    hass: HomeAssistant,
    coordinator: DeyeDataUpdateCoordinator,
    freezer: FrozenDateTimeFactory,
) -> None:
    """Cloud list reconciliation must not restore stale sensors to available."""
    await _stall(coordinator, freezer)
    coordinator.update_device_availability(True)
    coordinator.sync_cloud_device(
        DeyeDevice(coordinator.device.client, coordinator.device.info)
    )
    assert coordinator.device.available
    assert not coordinator.data.available
    snapshot = _coordinator_snapshot(coordinator)
    assert snapshot["state_updates_stalled"] is True
    assert snapshot["state_query_failures"] == STATE_QUERY_FAILURES_BEFORE_RECOVERY
    assert snapshot["last_state_received"] is None


async def test_offline_device_does_not_trigger_recovery(
    hass: HomeAssistant,
    coordinator: DeyeDataUpdateCoordinator,
    freezer: FrozenDateTimeFactory,
) -> None:
    """A deliberately unplugged device must not repeatedly reload the account."""
    coordinator.update_device_availability(False)
    with patch.object(hass.config_entries, "async_schedule_reload") as reload_entry:
        await coordinator.async_refresh()
        await _stall(coordinator, freezer)
        await _advance(hass, freezer, STATE_RECOVERY_DELAY + 1)
    assert coordinator.state_query_failures == 0
    assert not coordinator.state_updates_stalled
    reload_entry.assert_not_called()


async def test_cloud_offline_cancels_pending_recovery(
    hass: HomeAssistant,
    coordinator: DeyeDataUpdateCoordinator,
    freezer: FrozenDateTimeFactory,
) -> None:
    """Offline cloud metadata cancels recovery even if entities are unavailable."""
    with patch.object(hass.config_entries, "async_schedule_reload") as reload_entry:
        await _stall(coordinator, freezer)
        coordinator.sync_cloud_device(
            DeyeDevice(
                coordinator.device.client, coordinator.device.info | {"online": False}
            )
        )
        assert coordinator._recovery_unsub is None
        await _advance(hass, freezer, STATE_RECOVERY_DELAY + 1)
    reload_entry.assert_not_called()


async def test_fast_reply_during_request_does_not_leave_a_deadline(
    hass: HomeAssistant,
    coordinator: DeyeDataUpdateCoordinator,
    freezer: FrozenDateTimeFactory,
) -> None:
    """A reply can arrive before the HTTP request finishes."""
    coordinator.update_device_state(coordinator.data.reported_state.copy())

    async def request_refresh() -> None:
        coordinator.update_device_state(coordinator.data.reported_state.copy())

    with patch.object(
        coordinator.device, "request_refresh", side_effect=request_refresh
    ):
        await coordinator.async_refresh()
    await _advance(hass, freezer, STATE_QUERY_TIMEOUT + 1)
    assert coordinator.state_query_failures == 0
    assert coordinator._state_query_unsub is None


async def test_repeated_polls_do_not_postpone_pending_deadline(
    hass: HomeAssistant,
    coordinator: DeyeDataUpdateCoordinator,
    freezer: FrozenDateTimeFactory,
) -> None:
    """Extra refresh requests cannot indefinitely postpone stall detection."""
    await _advance(hass, freezer, STATE_QUERY_TIMEOUT / 2)
    await coordinator.async_refresh()
    await _advance(hass, freezer, STATE_QUERY_TIMEOUT / 2 + 1)
    assert coordinator.state_query_failures == 1


async def test_shutdown_cancels_deadlines_and_reload(
    hass: HomeAssistant,
    coordinator: DeyeDataUpdateCoordinator,
    freezer: FrozenDateTimeFactory,
) -> None:
    """An unloaded coordinator must not reload the account later."""
    with patch.object(hass.config_entries, "async_schedule_reload") as reload_entry:
        await _stall(coordinator, freezer)
        await coordinator.async_refresh()
        await coordinator.async_shutdown()
        await _advance(hass, freezer, STATE_QUERY_TIMEOUT + STATE_RECOVERY_DELAY + 1)
    assert coordinator._state_query_unsub is None
    assert coordinator._recovery_unsub is None
    reload_entry.assert_not_called()


async def test_recovery_cooldown_survives_coordinator_replacement(
    hass: HomeAssistant,
    coordinator: DeyeDataUpdateCoordinator,
    freezer: FrozenDateTimeFactory,
) -> None:
    """Reloads and multiple devices on one account share the same cooldown."""
    with patch.object(hass.config_entries, "async_schedule_reload") as reload_entry:
        await _stall(coordinator, freezer)
        await _advance(hass, freezer, STATE_RECOVERY_DELAY + 1)
        assert reload_entry.call_count == 1
        await coordinator.async_shutdown()
        replacement = DeyeDataUpdateCoordinator(
            hass, coordinator.config_entry, coordinator.device
        )
        try:
            await replacement._async_setup()
            await replacement.async_refresh()
            await _stall(replacement, freezer)
            await _advance(hass, freezer, STATE_RECOVERY_DELAY + 1)
            assert reload_entry.call_count == 1
            assert coordinator.config_entry is not None
            assert coordinator.config_entry.entry_id in hass.data[DATA_STATE_RECOVERY]
            await _advance(hass, freezer, STATE_RECOVERY_COOLDOWN)
            assert reload_entry.call_count == 2
        finally:
            await replacement.async_shutdown()


async def test_devices_on_same_account_coalesce_recovery(
    hass: HomeAssistant,
    coordinator: DeyeDataUpdateCoordinator,
    freezer: FrozenDateTimeFactory,
) -> None:
    """Two stalled devices sharing one MQTT pool schedule only one reload."""
    other = DeyeDataUpdateCoordinator(
        hass,
        coordinator.config_entry,
        DeyeDevice(
            coordinator.device.client,
            coordinator.device.info | {"device_id": "device-2"},
        ),
    )
    try:
        await other._async_setup()
        await other.async_refresh()
        with patch.object(hass.config_entries, "async_schedule_reload") as reload_entry:
            for failure in range(STATE_QUERY_FAILURES_BEFORE_RECOVERY):
                if failure:
                    await coordinator.async_refresh()
                    await other.async_refresh()
                await _advance(hass, freezer, STATE_QUERY_TIMEOUT + 1)
            await _advance(hass, freezer, STATE_RECOVERY_DELAY + 1)
            reload_entry.assert_called_once()
    finally:
        await other.async_shutdown()


async def test_unload_during_request_does_not_leave_a_deadline(
    hass: HomeAssistant,
    coordinator: DeyeDataUpdateCoordinator,
    freezer: FrozenDateTimeFactory,
) -> None:
    """A request that finishes after shutdown must not arm another timer."""
    started = asyncio.Event()
    finish = asyncio.Event()

    async def request_refresh() -> None:
        started.set()
        await finish.wait()

    with patch.object(
        coordinator.device, "request_refresh", side_effect=request_refresh
    ):
        task = hass.async_create_task(coordinator.async_refresh())
        await started.wait()
        await coordinator.async_shutdown()
        finish.set()
        await task
    await _advance(hass, freezer, STATE_QUERY_TIMEOUT + 1)
    assert coordinator._state_query_unsub is None
    assert coordinator.state_query_failures == 0
