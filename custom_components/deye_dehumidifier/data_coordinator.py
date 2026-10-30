"""Data update coordinator for Deye dehumidifier devices."""

import asyncio
from datetime import datetime, timedelta
import logging
from typing import NamedTuple, override

from libdeye.client import DeyeClient, DeyeDevice
from libdeye.cloud_api import (
    DeyeApiResponseDeviceInfo,
    DeyeCloudApiCannotConnectError,
    DeyeCloudApiInvalidAuthError,
    DeyeDeviceTransport,
)
from libdeye.device_state import DeyeDeviceState

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import CALLBACK_TYPE, HomeAssistant, callback
from homeassistant.exceptions import ConfigEntryAuthFailed, ConfigEntryNotReady
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers.event import async_call_later
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed
from homeassistant.util import dt as dt_util

from .const import (
    DATA_STATE_RECOVERY,
    DEVICE_LIST_UPDATE_INTERVAL,
    DOMAIN,
    STATE_QUERY_FAILURES_BEFORE_RECOVERY,
    STATE_QUERY_TIMEOUT,
    STATE_RECOVERY_COOLDOWN,
    STATE_RECOVERY_DELAY,
    is_dehumidifier_product_type,
)
from .subentries import configured_device_ids

_LOGGER = logging.getLogger(__name__)

_HTTP_TOO_MANY_REQUESTS = 429
_DEFAULT_RATE_LIMIT_RETRY_AFTER = 60.0


def _retry_after_from_exception(err: BaseException) -> float | None:
    """Return a backoff in seconds when a rate-limit (HTTP 429) is detected."""
    seen: set[int] = set()
    current: BaseException | None = err
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        status = getattr(current, "status", None)
        if status is None:
            status = getattr(current, "status_code", None)
        if status == _HTTP_TOO_MANY_REQUESTS:
            headers = getattr(current, "headers", None)
            retry_after = None
            if headers is not None:
                retry_after = headers.get("Retry-After") or headers.get("retry-after")
            if retry_after is not None:
                try:
                    return max(float(retry_after), 0.0)
                except TypeError, ValueError:
                    pass
            return _DEFAULT_RATE_LIMIT_RETRY_AFTER
        current = current.__cause__ or current.__context__
    return None


class DeyeDeviceData(NamedTuple):
    """Coordinator data for a single Deye device."""

    reported_state: DeyeDeviceState
    state: DeyeDeviceState
    available: bool


class DeyeDataUpdateCoordinator(DataUpdateCoordinator[DeyeDeviceData]):
    """Coordinator that keeps a Deye device's state in sync."""

    def __init__(
        self,
        hass: HomeAssistant,
        config_entry: ConfigEntry,
        device: DeyeDevice,
    ) -> None:
        """Initialize the coordinator."""
        super().__init__(
            hass,
            _LOGGER,
            config_entry=config_entry,
            name=f"{device.name} ({device.device_id})",
            update_interval=timedelta(seconds=30),
            always_update=False,
        )
        self.device = device
        self.state_update_muted: CALLBACK_TYPE | None = None
        self._unsubscribers: list[CALLBACK_TYPE] = []
        self._unavailable_logged = False
        self.last_state_received: datetime | None = None
        self.state_query_failures = 0
        self.state_updates_stalled = False
        self._state_generation = 0
        self._state_query_unsub: CALLBACK_TYPE | None = None
        self._recovery_unsub: CALLBACK_TYPE | None = None

    @override
    async def _async_setup(self) -> None:
        """Set up the coordinator and subscribe to MQTT state changes."""
        await self.device.ensure_connected()
        reported_state = self.device.reported_state
        available = self.device.available
        self.data = DeyeDeviceData(
            reported_state=reported_state,
            state=reported_state.copy(),
            available=available,
        )
        self._log_availability(available)
        self._unsubscribers.append(
            self.device.subscribe(
                on_state=self.update_device_state,
                on_availability=self.update_device_availability,
            )
        )
        if self.config_entry is not None:
            self.config_entry.async_on_unload(self.unsubscribe)

    @callback
    def unsubscribe(self) -> None:
        """Remove MQTT subscriptions so callbacks do not leak after unload."""
        while self._unsubscribers:
            self._unsubscribers.pop()()
        if self.state_update_muted:
            self.state_update_muted()
            self.state_update_muted = None
        self._cancel_state_query()
        self._cancel_recovery()

    @override
    async def async_shutdown(self) -> None:
        """Unsubscribe MQTT callbacks and shut down the coordinator."""
        self.unsubscribe()
        await super().async_shutdown()

    def mute_state_update_for_a_while(self) -> None:
        """Mute subscription for a while to avoid state bouncing."""
        if self.state_update_muted:
            self.state_update_muted()

        @callback
        def unmute(now: datetime) -> None:
            self.state_update_muted = None

        self.state_update_muted = async_call_later(self.hass, 10, unmute)

    def update_device_state(
        self, state: DeyeDeviceState, *, available: bool = True
    ) -> None:
        """Will be called when received new DeyeDeviceState."""
        self._state_generation += 1
        self.last_state_received = dt_util.utcnow()
        if self.state_query_failures:
            _LOGGER.info("State messages recovered for %s", self.name)
        self.state_query_failures = 0
        self.state_updates_stalled = False
        self._cancel_state_query()
        self._cancel_recovery()
        # A device can resume reporting without another MQTT online event.
        self.device.available = available
        self._log_availability(available)
        if self.state_update_muted:
            if self.data.available != available:
                self.update_device_availability(available)
            return
        self.async_set_updated_data(
            DeyeDeviceData(
                reported_state=state,
                state=state.copy(),
                available=available,
            )
        )

    def update_device_availability(self, available: bool) -> None:
        """Will be called when received device availability change."""
        self.device.available = available
        if not available:
            self._cancel_state_query()
            self._cancel_recovery()
            self.state_query_failures = 0
        available = available and not self.state_updates_stalled
        self._log_availability(available)
        self.async_set_updated_data(
            DeyeDeviceData(
                reported_state=self.data.reported_state,
                state=self.data.state,
                available=available,
            )
        )

    @callback
    def sync_cloud_device(self, device: DeyeDevice) -> None:
        """Reconcile online status while keeping the subscribed device alive.

        The cloud list creates new wrappers on each poll. Copy their metadata
        onto the existing wrapper, but keep its MQTT subscriptions and state.
        This also repairs a missed online/offline MQTT notification.
        """
        self.device.info = device.info
        available = bool(device.info.get("online", self.device.available))
        if (
            available != self.device.available
            or (available and not self.state_updates_stalled) != self.data.available
        ):
            self.update_device_availability(available)
        else:
            self.device.available = available

    @callback
    def _cancel_state_query(self) -> None:
        """Cancel the deadline for a pending state reply."""
        if self._state_query_unsub is not None:
            self._state_query_unsub()
            self._state_query_unsub = None

    @callback
    def _cancel_recovery(self) -> None:
        """Cancel a pending recovery when state resumes or the device unloads."""
        if self._recovery_unsub is not None:
            self._recovery_unsub()
            self._recovery_unsub = None

    @callback
    def _state_query_expired(self, _now: datetime) -> None:
        """Count missing replies to Classic/Combo MQTT queries."""
        self._state_query_unsub = None
        if not self.device.available:
            return
        self.state_query_failures += 1
        if self.state_query_failures < STATE_QUERY_FAILURES_BEFORE_RECOVERY:
            return
        if not self.state_updates_stalled:
            _LOGGER.warning(
                "No state reply from %s after %s consecutive refresh requests; "
                "cloud online status does not confirm state reception",
                self.name,
                self.state_query_failures,
            )
        self.state_updates_stalled = True
        self.update_device_availability(True)
        if self._recovery_unsub is None and self.config_entry is not None:
            self._recovery_unsub = async_call_later(
                self.hass, STATE_RECOVERY_DELAY, self._recover_state_updates
            )

    @callback
    def _recover_state_updates(self, _now: datetime) -> None:
        """Recreate pooled MQTT connections, with an account-wide cooldown."""
        self._recovery_unsub = None
        if (
            self.config_entry is None
            or not self.device.available
            or not self.state_updates_stalled
        ):
            return
        # Keep this across runtime_data replacement during a config-entry reload.
        recoveries: dict[str, float] = self.hass.data.setdefault(
            DATA_STATE_RECOVERY, {}
        )
        entry_id = self.config_entry.entry_id
        now = self.hass.loop.time()
        if (last_recovery := recoveries.get(entry_id)) is not None:
            remaining = STATE_RECOVERY_COOLDOWN - (now - last_recovery)
            if remaining > 0:
                self._recovery_unsub = async_call_later(
                    self.hass, remaining, self._recover_state_updates
                )
                return
        recoveries[entry_id] = now
        _LOGGER.warning(
            "Reloading Deye account to recover missing state messages for %s", self.name
        )
        self.hass.config_entries.async_schedule_reload(entry_id)

    def _log_availability(self, available: bool) -> None:
        """Log once when a device becomes unavailable, and once when it recovers."""
        if available:
            if self._unavailable_logged:
                _LOGGER.info("%s is available again", self.name)
                self._unavailable_logged = False
            return
        if not self._unavailable_logged:
            _LOGGER.info("%s is unavailable", self.name)
            self._unavailable_logged = True

    def sync_reported_state_after_publish(self) -> None:
        """Align reported state with the desired state after a successful publish."""
        self.data = DeyeDeviceData(
            reported_state=self.data.state.copy(),
            state=self.data.state,
            available=self.data.available,
        )

    @override
    async def _async_update_data(self) -> DeyeDeviceData:
        """Poll device state. Some Deye devices have a long heartbeat period.

        Fog reads properties directly over HTTP: a RealData request does not
        guarantee an MQTT reply. Classic/Combo query and await MQTT state.
        """
        if self.state_update_muted:
            return self.data

        generation = self._state_generation
        reported_state: DeyeDeviceState | None
        try:
            async with asyncio.timeout(STATE_QUERY_TIMEOUT):
                if self.device.transport is DeyeDeviceTransport.FOG:
                    reported_state = await self.device.refresh()
                else:
                    reported_state = await self.device.request_refresh()
        except DeyeCloudApiInvalidAuthError as err:
            raise ConfigEntryAuthFailed from err
        except (DeyeCloudApiCannotConnectError, OSError) as err:
            raise UpdateFailed(
                f"Error communicating with {self.name}: {str(err) or type(err).__name__}",
                retry_after=_retry_after_from_exception(err),
            ) from err

        if self._shutdown_requested:
            return self.data
        if reported_state is None:
            if generation == self._state_generation and self._state_query_unsub is None:
                self._state_query_unsub = async_call_later(
                    self.hass, STATE_QUERY_TIMEOUT, self._state_query_expired
                )
            return self.data
        self.update_device_state(
            reported_state,
            available=(
                self.device.available
                if self.device.transport is DeyeDeviceTransport.FOG
                else True
            ),
        )
        return self.data


class DeyeDeviceListCoordinator(DataUpdateCoordinator[list[DeyeApiResponseDeviceInfo]]):
    """Periodically refresh the cloud device list and sync local devices."""

    def __init__(
        self,
        hass: HomeAssistant,
        config_entry: ConfigEntry,
        client: DeyeClient,
        coordinator_map: dict[str, DeyeDataUpdateCoordinator],
        device_list: list[DeyeApiResponseDeviceInfo],
    ) -> None:
        """Initialize the device-list coordinator."""
        super().__init__(
            hass,
            _LOGGER,
            config_entry=config_entry,
            name=f"{DOMAIN} device list",
            update_interval=DEVICE_LIST_UPDATE_INTERVAL,
        )
        self._client = client
        self.coordinator_map = coordinator_map
        self.device_list = device_list
        self._initial_sync = True
        self._last_devices: list[DeyeDevice] = []

    @override
    async def _async_update_data(self) -> list[DeyeApiResponseDeviceInfo]:
        """Fetch the cloud device list and add or remove local devices."""
        try:
            devices = [
                device
                for device in await self._client.list_devices()
                if is_dehumidifier_product_type(device.info["product_type"])
            ]
        except DeyeCloudApiInvalidAuthError as err:
            raise ConfigEntryAuthFailed from err
        except DeyeCloudApiCannotConnectError as err:
            raise UpdateFailed("Cannot connect to Deye Cloud") from err

        self._last_devices = devices
        await self._async_sync_devices(devices)
        return list(self.device_list)

    async def async_sync_configured_devices(self) -> None:
        """Create coordinators for subentries using the last successful list."""
        if not self._last_devices:
            return
        await self._async_sync_devices(self._last_devices)

    def mark_initial_sync_done(self) -> None:
        """Treat later device-list polls as incremental discovery."""
        self._initial_sync = False

    async def _async_sync_devices(self, devices: list[DeyeDevice]) -> None:
        """Create coordinators for configured devices and drop stale ones."""
        configured = (
            configured_device_ids(self.config_entry)
            if self.config_entry is not None
            else set()
        )
        current_ids = {device.device_id for device in devices}

        for device_id in list(self.coordinator_map):
            if device_id not in current_ids:
                await self._async_remove_stale_device(device_id)
            elif device_id not in configured:
                coordinator = self.coordinator_map.pop(device_id, None)
                if coordinator is not None:
                    await coordinator.async_shutdown()

        for device in devices:
            if coordinator := self.coordinator_map.get(device.device_id):
                coordinator.sync_cloud_device(device)
            elif device.device_id in configured:
                await self._async_add_device(device)

        missing = configured - current_ids
        if missing and self._initial_sync:
            for device_id in sorted(missing):
                _LOGGER.warning(
                    "Dehumidifier %s is configured but not on the Deye account",
                    device_id,
                )

        self.device_list.clear()
        self.device_list.extend(device.info for device in devices)

    async def _async_add_device(self, device: DeyeDevice) -> None:
        """Create a coordinator for a configured dehumidifier."""
        if self.config_entry is None:
            raise UpdateFailed("Config entry is missing")
        coordinator = DeyeDataUpdateCoordinator(self.hass, self.config_entry, device)
        try:
            await coordinator.async_config_entry_first_refresh()
        except ConfigEntryAuthFailed:
            await coordinator.async_shutdown()
            raise
        except ConfigEntryNotReady:
            await coordinator.async_shutdown()
            if self._initial_sync:
                raise
            _LOGGER.exception(
                "Failed to set up newly discovered dehumidifier %s (%s)",
                device.name,
                device.device_id,
            )
            return
        self.coordinator_map[device.device_id] = coordinator
        if not self._initial_sync:
            _LOGGER.info(
                "Discovered dehumidifier %s (%s)", device.name, device.device_id
            )

    async def _async_remove_stale_device(self, device_id: str) -> None:
        """Remove a dehumidifier that is no longer on the Deye account."""
        coordinator = self.coordinator_map.pop(device_id, None)
        mac: str | None = None
        if coordinator is not None:
            mac = coordinator.device.info["mac"]
            await coordinator.async_shutdown()
        else:
            for info in self.device_list:
                if info["device_id"] == device_id:
                    mac = info["mac"]
                    break
        if mac is None or self.config_entry is None:
            return

        device_registry = dr.async_get(self.hass)
        get_by_identifier = getattr(
            device_registry, "async_get_device_by_identifier", None
        )
        if callable(get_by_identifier):
            device_entry = get_by_identifier((DOMAIN, mac), self.config_entry.entry_id)
        else:
            device_entry = device_registry.async_get_device(identifiers={(DOMAIN, mac)})
        if device_entry is not None:
            device_registry.async_remove_device(device_entry.id)
            _LOGGER.info("Removed stale dehumidifier %s (%s)", mac, device_id)
