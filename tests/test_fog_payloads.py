"""Only measured Fog telemetry may renew environmental data freshness."""

from unittest.mock import MagicMock

from freezegun.api import FrozenDateTimeFactory
from libdeye.mqtt_client import DeyeFogMqttClient
import pytest

from custom_components.deye_dehumidifier.data_coordinator import (
    DeyeDataUpdateCoordinator,
)
from custom_components.deye_dehumidifier.diagnostics import _coordinator_snapshot
from custom_components.deye_dehumidifier.humidifier import DeyeDehumidifier
from custom_components.deye_dehumidifier.sensor import (
    DeyeHumiditySensor,
    DeyeTemperatureSensor,
)
from homeassistant.core import HomeAssistant
from homeassistant.util import dt as dt_util
from tests.test_state_recovery import _advance


@pytest.mark.parametrize("properties", [{}, {"RealData": 1}, {"Power": 0}])
async def test_fog_ack_without_measurements_cannot_replace_or_skip_http_telemetry(
    fog_coordinator: tuple[DeyeDataUpdateCoordinator, MagicMock],
    properties: dict[str, int],
) -> None:
    """Control acknowledgements must not fabricate humidity or stop the HTTP GET."""
    coordinator, api = fog_coordinator
    mqtt = coordinator.device.client._mqtt_by_type[DeyeFogMqttClient]
    last_received = coordinator.last_state_received

    async def acknowledge(device_id: str) -> None:
        payload = {
            "device_id": device_id,
            "biz_code": "device_data",
            "data": {"message_type": "thing_property", "properties": properties},
        }
        for handler in mqtt._subscribers[""].copy():
            handler(payload)
        assert coordinator.data.state.environment_humidity == 47
        assert coordinator.last_state_received == last_received

    api.poll_fog_platform_device_properties.side_effect = acknowledge
    api.get_fog_platform_device_properties.return_value = {
        "CurrentEnvironmentalHumidity": 64,
        "CurrentAmbientTemperature": 28,
    }
    await coordinator.async_refresh()
    assert coordinator.last_update_success
    assert api.get_fog_platform_device_properties.await_count == 2
    assert coordinator.data.state.environment_humidity == 64
    assert coordinator.data.state.environment_temperature == 28


@pytest.mark.parametrize(
    "properties",
    [
        {},
        {"CurrentEnvironmentalHumidity": None},
        {"CurrentEnvironmentalHumidity": "nan"},
        {"CurrentAmbientTemperature": False},
    ],
)
async def test_invalid_http_properties_cannot_keep_cached_measurements_fresh(
    hass: HomeAssistant,
    fog_coordinator: tuple[DeyeDataUpdateCoordinator, MagicMock],
    freezer: FrozenDateTimeFactory,
    properties: dict[str, object],
) -> None:
    """An empty successful HTTP response is neither a measurement nor recovery."""
    coordinator, api = fog_coordinator
    last_received = coordinator.last_state_received
    api.get_fog_platform_device_properties.return_value = properties
    await coordinator.async_refresh()
    assert not coordinator.last_update_success
    assert coordinator.data.state.environment_humidity == 47
    assert coordinator.device.reported_state.environment_humidity == 47
    assert coordinator.last_state_received == last_received
    await _advance(hass, freezer, 181)
    assert coordinator.state_data_stale
    assert not coordinator.data.available
    api.get_fog_platform_device_properties.return_value = {
        "CurrentEnvironmentalHumidity": 65,
        "CurrentAmbientTemperature": 28,
    }
    await coordinator.async_refresh()
    assert coordinator.last_update_success
    assert coordinator.data.available
    assert coordinator.data.state.environment_humidity == 65


@pytest.mark.parametrize(
    ("properties", "humidity", "temperature"),
    [
        ({"CurrentEnvironmentalHumidity": 64}, 64, 28),
        ({"CurrentAmbientTemperature": 29}, 47, 29),
        ({"CurrentEnvironmentalHumidity": 0}, 0, 28),
    ],
)
@pytest.mark.parametrize("source", ["mqtt", "http"])
async def test_partial_measurements_preserve_other_environmental_reading(
    fog_coordinator: tuple[DeyeDataUpdateCoordinator, MagicMock],
    properties: dict[str, int],
    humidity: int,
    temperature: int,
    source: str,
) -> None:
    """A valid partial reading must not reset the missing reading to SDK defaults."""
    coordinator, api = fog_coordinator
    api.get_fog_platform_device_properties.return_value = {
        "CurrentEnvironmentalHumidity": 47,
        "CurrentAmbientTemperature": 28,
    }
    await coordinator.async_refresh()
    if source == "http":
        api.get_fog_platform_device_properties.return_value = properties
        await coordinator.async_refresh()
    else:
        mqtt = coordinator.device.client._mqtt_by_type[DeyeFogMqttClient]
        payload = {
            "device_id": coordinator.device.device_id,
            "biz_code": "device_data",
            "data": {"message_type": "thing_property", "properties": properties},
        }
        for handler in mqtt._subscribers[""].copy():
            handler(payload)
    assert coordinator.data.state.environment_humidity == humidity
    assert coordinator.data.state.environment_temperature == temperature
    assert coordinator.device.reported_state.environment_humidity == humidity
    assert coordinator.device.state.environment_temperature == temperature


@pytest.mark.parametrize("phase", ["report_request", "http_response"])
@pytest.mark.parametrize(
    ("key", "field"),
    [
        ("CurrentAmbientTemperature", "environment_temperature"),
        ("CurrentEnvironmentalHumidity", "environment_humidity"),
    ],
)
async def test_partial_mqtt_reading_cannot_starve_other_http_measurements(
    fog_coordinator: tuple[DeyeDataUpdateCoordinator, MagicMock],
    phase: str,
    key: str,
    field: str,
) -> None:
    """Repeated partial pushes must not stop the other reading or revert newer data."""
    coordinator, api = fog_coordinator
    mqtt = coordinator.device.client._mqtt_by_type[DeyeFogMqttClient]

    readings = {
        "CurrentEnvironmentalHumidity": 64,
        "CurrentAmbientTemperature": 28,
        "Power": 1,
    }
    pushed_value = 35

    def push_reading() -> None:
        payload = {
            "device_id": coordinator.device.device_id,
            "biz_code": "device_data",
            "data": {
                "message_type": "thing_property",
                "properties": {key: pushed_value, "Power": 0},
            },
        }
        for handler in mqtt._subscribers[""].copy():
            handler(payload)

    async def request_report(device_id: str) -> None:
        if phase == "report_request":
            push_reading()

    async def read_properties(device_id: str) -> dict[str, int]:
        if phase == "http_response":
            push_reading()
        return dict(readings)

    api.poll_fog_platform_device_properties.side_effect = request_report
    api.get_fog_platform_device_properties.side_effect = read_properties
    for index in range(3):
        readings["CurrentEnvironmentalHumidity"] = 64 + index
        readings["CurrentAmbientTemperature"] = 28 + index
        pushed_value = 35 + index
        await coordinator.async_refresh()
        assert coordinator.last_update_success
        expected = {
            "environment_humidity": readings["CurrentEnvironmentalHumidity"],
            "environment_temperature": readings["CurrentAmbientTemperature"],
            "power_switch": True,
        }
        if phase == "http_response":
            expected[field] = pushed_value
            expected["power_switch"] = False
        for name, value in expected.items():
            assert getattr(coordinator.data.state, name) == value
            assert getattr(coordinator.device.reported_state, name) == value
        assert api.get_fog_platform_device_properties.await_count == index + 2


@pytest.mark.parametrize("updated_field", ["humidity", "temperature"])
async def test_partial_reports_cannot_keep_other_measurement_available(
    hass: HomeAssistant,
    fog_coordinator: tuple[DeyeDataUpdateCoordinator, MagicMock],
    freezer: FrozenDateTimeFactory,
    updated_field: str,
) -> None:
    """One arriving measurement cannot hide another measurement's expired cache."""
    coordinator, api = fog_coordinator
    humidity = DeyeHumiditySensor(coordinator, coordinator.device.info)
    temperature = DeyeTemperatureSensor(coordinator, coordinator.device.info)
    dehumidifier = DeyeDehumidifier(coordinator, coordinator.device.info)
    api.get_fog_platform_device_properties.return_value = (
        {"CurrentAmbientTemperature": 29}
        if updated_field == "temperature"
        else {"CurrentEnvironmentalHumidity": 64}
    )
    for _ in range(4):
        await _advance(hass, freezer, 61)
        await coordinator.async_refresh()
    assert coordinator.data.available
    if updated_field == "temperature":
        assert temperature.available
        assert temperature.native_value == 29
        assert not humidity.available
        assert humidity.native_value == 47
        assert dehumidifier.current_humidity is None
    else:
        assert humidity.available
        assert humidity.native_value == 64
        assert dehumidifier.current_humidity == 64
        assert not temperature.available
        assert temperature.native_value == 27
    api.get_fog_platform_device_properties.return_value = {
        "CurrentEnvironmentalHumidity": 64,
        "CurrentAmbientTemperature": 29,
    }
    await coordinator.async_refresh()
    assert humidity.available
    assert humidity.native_value == 64
    assert dehumidifier.current_humidity == 64
    assert temperature.available
    assert temperature.native_value == 29


async def test_individual_measurement_expiry_notifies_entities_and_unloads_cleanly(
    hass: HomeAssistant,
    fog_coordinator: tuple[DeyeDataUpdateCoordinator, MagicMock],
    freezer: FrozenDateTimeFactory,
) -> None:
    """Missing humidity expires at its own deadline even between successful polls."""
    coordinator, api = fog_coordinator
    humidity = DeyeHumiditySensor(coordinator, coordinator.device.info)
    temperature = DeyeTemperatureSensor(coordinator, coordinator.device.info)
    humidity_received = coordinator.last_measurement_received["environment_humidity"]
    assert humidity_received is not None
    await _advance(hass, freezer, 60)
    api.get_fog_platform_device_properties.return_value = {
        "CurrentAmbientTemperature": 29,
    }
    await coordinator.async_refresh()
    coordinator.update_interval = None
    snapshots = []
    remove = coordinator.async_add_listener(
        lambda: snapshots.append((humidity.available, temperature.available))
    )
    try:
        await _advance(hass, freezer, 119)
        assert not snapshots
        await _advance(hass, freezer, 2)
        assert snapshots == [(False, True)]
        snapshot = _coordinator_snapshot(coordinator)
        assert snapshot["last_measurement_received"]["environment_humidity"] == (
            humidity_received.isoformat()
        )
        assert snapshot["measurement_data_stale"] == {
            "environment_humidity": True,
            "environment_temperature": False,
        }
        assert coordinator._measurement_expiry_unsub is not None
        await coordinator.async_shutdown()
        assert coordinator._measurement_expiry_unsub is None
        coordinator._measurements_expired(dt_util.utcnow())
        assert coordinator._measurement_expiry_unsub is None
        assert snapshots == [(False, True)]
    finally:
        remove()
