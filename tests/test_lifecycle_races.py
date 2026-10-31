"""Regression tests for cloud-list requests crossing device unload/discovery."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from aiohttp import ClientResponseError
from libdeye.cloud_api import DeyeCloudApiCannotConnectError
import pytest
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.deye_dehumidifier import async_setup_entry
from custom_components.deye_dehumidifier.const import DOMAIN
from custom_components.deye_dehumidifier.data_coordinator import (
    DeyeDeviceListCoordinator,
)
from homeassistant.config_entries import ConfigEntryState
from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import UpdateFailed
from tests.helpers import MOCK_CONFIG, MOCK_DEVICE_INFO, FakeDeyeDevice
from tests.test_device_lifecycle import _cloud_device, _make_list_coordinator


@pytest.mark.parametrize("phase", ["device_list", "device_connection"])
async def test_cancelled_account_setup_disconnects_its_clients(
    hass: HomeAssistant,
    mock_libdeye: tuple[MagicMock, FakeDeyeDevice],
    phase: str,
) -> None:
    """Cancelling either setup phase must release the account's pooled clients."""
    client, device = mock_libdeye
    entered = asyncio.Event()

    async def blocked() -> None:
        entered.set()
        await asyncio.Event().wait()

    if phase == "device_list":
        client.list_devices.side_effect = blocked
    else:
        device.ensure_connected = AsyncMock(side_effect=blocked)
    entry = MockConfigEntry(domain=DOMAIN, data=MOCK_CONFIG, version=2)
    entry.add_to_hass(hass)
    entry._async_set_state(hass, ConfigEntryState.SETUP_IN_PROGRESS, None)
    task = hass.async_create_task(async_setup_entry(hass, entry))
    try:
        async with asyncio.timeout(5):
            while not entered.is_set():
                if task.done():
                    await task
                await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        client.disconnect.assert_called_once()
        assert device._on_state is None
        assert device._on_availability is None
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_cloud_response_after_shutdown_does_not_remove_devices(
    hass: HomeAssistant,
) -> None:
    """A late response from the old entry cannot alter the active registry/map."""
    entry = MockConfigEntry(domain=DOMAIN, data=MOCK_CONFIG, version=2)
    entry.add_to_hass(hass)
    entered, finish = asyncio.Event(), asyncio.Event()

    async def slow_list() -> list:
        entered.set()
        await finish.wait()
        return []

    client = MagicMock()
    client.list_devices = AsyncMock(side_effect=slow_list)
    existing = MagicMock()
    existing.async_shutdown = AsyncMock()
    existing.device.info = MOCK_DEVICE_INFO
    devices = [MOCK_DEVICE_INFO]
    coordinators = {MOCK_DEVICE_INFO["device_id"]: existing}
    coordinator = DeyeDeviceListCoordinator(hass, entry, client, coordinators, devices)
    task = hass.async_create_task(coordinator.async_refresh())
    try:
        await entered.wait()
        await coordinator.async_shutdown()
        finish.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert coordinators == {MOCK_DEVICE_INFO["device_id"]: existing}
        assert devices == [MOCK_DEVICE_INFO]
        assert not coordinator._last_devices
        existing.async_shutdown.assert_not_awaited()
    finally:
        finish.set()
        await asyncio.gather(task, return_exceptions=True)
        await coordinator.async_shutdown()


@pytest.mark.parametrize("cancel", [False, True])
async def test_unfinished_device_setup_is_disposed_on_unload_or_cancel(
    cancel: bool,
) -> None:
    """Discovery must not leak MQTT callbacks or commit a device after unload."""
    coordinator = _make_list_coordinator("dev-new")
    entered, finish = asyncio.Event(), asyncio.Event()

    async def setup_device() -> None:
        entered.set()
        await finish.wait()

    child = MagicMock()
    child.async_config_entry_first_refresh = AsyncMock(side_effect=setup_device)
    child.async_shutdown = AsyncMock()
    with patch(
        "custom_components.deye_dehumidifier.data_coordinator.DeyeDataUpdateCoordinator",
        return_value=child,
    ):
        task = asyncio.create_task(
            coordinator._async_sync_devices([_cloud_device("dev-new", "mac-new")])
        )
        await entered.wait()
        if cancel:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        else:
            coordinator._shutdown_requested = True
            finish.set()
            await task
    child.async_shutdown.assert_awaited_once()
    assert not coordinator.coordinator_map
    assert not coordinator.device_list


async def test_overlapping_discovery_creates_one_device_coordinator() -> None:
    """Cloud refresh and subentry sync cannot subscribe the same device twice."""
    coordinator = _make_list_coordinator("dev-new")
    entered, finish = asyncio.Event(), asyncio.Event()

    async def setup_device() -> None:
        entered.set()
        await finish.wait()

    child = MagicMock()
    child.async_config_entry_first_refresh = AsyncMock(side_effect=setup_device)
    with patch(
        "custom_components.deye_dehumidifier.data_coordinator.DeyeDataUpdateCoordinator",
        return_value=child,
    ) as constructor:
        first = asyncio.create_task(
            coordinator._async_sync_devices([_cloud_device("dev-new", "mac-new")])
        )
        await entered.wait()
        device = _cloud_device("dev-new", "mac-new")
        second = asyncio.create_task(coordinator._async_sync_devices([device]))
        await asyncio.sleep(0)
        constructor.assert_called_once()
        finish.set()
        await first
        await second
    constructor.assert_called_once()
    child.sync_cloud_device.assert_called_once_with(device)


async def test_cloud_list_timeout_preserves_existing_devices() -> None:
    """A stalled account request has a deadline and cannot clear the device map."""
    coordinator = _make_list_coordinator("dev-1")
    existing = MagicMock()
    coordinator.coordinator_map["dev-1"] = existing

    async def never_finishes() -> None:
        await asyncio.Event().wait()

    coordinator._client.list_devices = AsyncMock(side_effect=never_finishes)
    with (
        patch(
            "custom_components.deye_dehumidifier.data_coordinator.DEVICE_LIST_QUERY_TIMEOUT",
            0,
        ),
        pytest.raises(UpdateFailed),
    ):
        await coordinator._async_update_data()
    assert coordinator.coordinator_map == {"dev-1": existing}


async def test_account_rate_limit_preserves_backoff() -> None:
    """Device discovery respects the cloud's Retry-After rather than retrying early."""
    coordinator = _make_list_coordinator("dev-1")
    error = DeyeCloudApiCannotConnectError()
    error.__cause__ = ClientResponseError(
        request_info=MagicMock(), history=(), status=429, headers={"Retry-After": "120"}
    )
    coordinator._client.list_devices = AsyncMock(side_effect=error)
    with pytest.raises(UpdateFailed) as raised:
        await coordinator._async_update_data()
    assert raised.value.retry_after == 120
