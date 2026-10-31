"""Pytest fixtures for the Deye Dehumidifier integration."""

from collections.abc import AsyncGenerator, Generator
from unittest.mock import AsyncMock, MagicMock, patch

from freezegun.api import FrozenDateTimeFactory
from libdeye.client import DeyeClient, DeyeDevice
from libdeye.cloud_api import DeyeCloudApi, DeyeIotPlatform
from libdeye.device_state import DeyeDeviceState
from libdeye.mqtt_client import DeyeFogMqttClient
import pytest
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.deye_dehumidifier.const import DOMAIN
from custom_components.deye_dehumidifier.data_coordinator import (
    DeyeDataUpdateCoordinator,
)
from homeassistant.core import HomeAssistant
from tests.helpers import (
    DEFAULT_STATE_HEX,
    MOCK_CONFIG,
    MOCK_DEVICE_INFO,
    FakeDeyeDevice,
)

pytest_plugins = "pytest_homeassistant_custom_component"


@pytest.fixture(params=[DeyeIotPlatform.Fog, DeyeIotPlatform.FogCombo])
async def fog_coordinator(
    hass: HomeAssistant,
    request: pytest.FixtureRequest,
    freezer: FrozenDateTimeFactory,
) -> AsyncGenerator[tuple[DeyeDataUpdateCoordinator, MagicMock]]:
    """Exercise libdeye's real HTTP properties path without network access."""
    api = MagicMock(spec=DeyeCloudApi)
    api.get_fog_platform_device_properties.return_value = {
        "CurrentEnvironmentalHumidity": 47,
        "CurrentAmbientTemperature": 27,
    }
    client = DeyeClient(api)
    client._mqtt_by_type[DeyeFogMqttClient] = DeyeFogMqttClient(api)
    device = DeyeDevice(client, MOCK_DEVICE_INFO | {"platform": request.param})
    entry = MockConfigEntry(domain=DOMAIN, data=MOCK_CONFIG, version=2)
    entry.add_to_hass(hass)
    coordinator = DeyeDataUpdateCoordinator(hass, entry, device)
    await coordinator._async_setup()
    await coordinator.async_refresh()
    yield coordinator, api
    await coordinator.async_shutdown()


@pytest.fixture(autouse=True)
def auto_enable_custom_integrations(enable_custom_integrations: None) -> None:
    """Enable loading this repo's custom_components in every test."""
    _ = enable_custom_integrations


@pytest.fixture
def mock_deye_cloud_api() -> Generator[MagicMock]:
    """Patch DeyeCloudApi used by config flow so tests never hit Deye Cloud."""
    with patch(
        "custom_components.deye_dehumidifier.config_flow.DeyeCloudApi",
        autospec=True,
    ) as mock_cls:
        api = mock_cls.return_value
        api.auth_token = "mock-auth-token"
        api.user_id = "user-123"
        api.authenticate = AsyncMock()
        yield api


@pytest.fixture
def mock_libdeye() -> Generator[tuple[MagicMock, FakeDeyeDevice]]:
    """Patch cloud API + DeyeClient (and its MQTT pool) used during setup."""
    device = FakeDeyeDevice(MOCK_DEVICE_INFO)
    with (
        patch(
            "custom_components.deye_dehumidifier.DeyeCloudApi",
            autospec=True,
        ) as mock_api_cls,
        patch(
            "custom_components.deye_dehumidifier.DeyeClient",
            autospec=True,
        ) as mock_client_cls,
    ):
        mock_api_cls.return_value.authenticate = AsyncMock()
        client = mock_client_cls.return_value
        client.list_devices = AsyncMock(return_value=[device])
        client.disconnect = MagicMock()
        yield client, device


@pytest.fixture
def fake_device_state() -> DeyeDeviceState:
    """Return a Classic-payload device state (20°C / 60% RH default hex)."""
    return DeyeDeviceState(DEFAULT_STATE_HEX)
