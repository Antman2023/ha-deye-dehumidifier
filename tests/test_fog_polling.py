"""Regression coverage for Fog devices that do not reply to RealData polls."""

from unittest.mock import MagicMock, patch

from freezegun.api import FrozenDateTimeFactory
from libdeye.cloud_api import DeyeCloudApiCannotConnectError

from custom_components.deye_dehumidifier.const import STATE_QUERY_TIMEOUT
from custom_components.deye_dehumidifier.data_coordinator import (
    DeyeDataUpdateCoordinator,
)
from custom_components.deye_dehumidifier.sensor import DeyeHumiditySensor
from homeassistant.core import HomeAssistant
from tests.test_state_recovery import _advance


async def test_fog_reads_fresh_http_state_without_mqtt_replies(
    hass: HomeAssistant,
    fog_coordinator: tuple[DeyeDataUpdateCoordinator, MagicMock],
    freezer: FrozenDateTimeFactory,
) -> None:
    """No MQTT traffic must not make a working Fog HTTP device unavailable."""
    coordinator, api = fog_coordinator
    sensor = DeyeHumiditySensor(coordinator, coordinator.device.info)
    assert sensor.native_value == 47
    with patch.object(hass.config_entries, "async_schedule_reload") as reload_entry:
        for humidity in (24, 25, 25, 26, 27):
            api.get_fog_platform_device_properties.return_value = {
                "CurrentEnvironmentalHumidity": humidity,
                "CurrentAmbientTemperature": 27,
            }
            await coordinator.async_refresh()
            await _advance(hass, freezer, STATE_QUERY_TIMEOUT + 1)
            assert sensor.native_value == humidity
            assert sensor.available
            assert coordinator.state_query_failures == 0
            assert not coordinator.state_updates_stalled
            assert coordinator._state_query_unsub is None
        reload_entry.assert_not_called()
    assert api.poll_fog_platform_device_properties.await_count == 6
    assert api.get_fog_platform_device_properties.await_count == 6
    assert coordinator.device.reported_state.environment_humidity == 27


async def test_fog_http_failure_recovers_on_next_successful_poll(
    hass: HomeAssistant,
    fog_coordinator: tuple[DeyeDataUpdateCoordinator, MagicMock],
    freezer: FrozenDateTimeFactory,
) -> None:
    """HTTP errors retain known state and clear on success without reload loops."""
    coordinator, api = fog_coordinator
    sensor = DeyeHumiditySensor(coordinator, coordinator.device.info)
    with patch.object(hass.config_entries, "async_schedule_reload") as reload_entry:
        api.get_fog_platform_device_properties.side_effect = (
            DeyeCloudApiCannotConnectError
        )
        await coordinator.async_refresh()
        assert not coordinator.last_update_success
        assert sensor.available
        assert coordinator.data.state.environment_humidity == 47
        await _advance(hass, freezer, 120)
        reload_entry.assert_not_called()
        api.get_fog_platform_device_properties.side_effect = None
        await coordinator.async_refresh()
        assert coordinator.last_update_success
        assert sensor.available
        assert coordinator._state_query_unsub is None


async def test_fog_http_properties_do_not_override_cloud_offline_status(
    fog_coordinator: tuple[DeyeDataUpdateCoordinator, MagicMock],
) -> None:
    """The cloud can return cached properties for an unplugged device."""
    coordinator, _ = fog_coordinator
    coordinator.update_device_availability(False)
    await coordinator.async_refresh()
    assert not coordinator.device.available
    assert not coordinator.data.available
