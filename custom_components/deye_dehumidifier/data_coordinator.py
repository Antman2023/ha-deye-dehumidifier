"""Data update coordinator for Deye dehumidifier devices."""

import asyncio
from collections.abc import Generator
from contextlib import contextmanager
from datetime import datetime, timedelta
import logging
from math import isfinite
from typing import NamedTuple, override

from libdeye.client import DeyeClient, DeyeDevice
from libdeye.cloud_api import (
    DeyeApiResponseDeviceInfo,
    DeyeCloudApiCannotConnectError,
    DeyeCloudApiInvalidAuthError,
    DeyeDeviceTransport,
)
from libdeye.device_state import DeyeDeviceState
from libdeye.mqtt_client import DeyeFogMqttClient

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import CALLBACK_TYPE, HomeAssistant, callback
from homeassistant.exceptions import ConfigEntryAuthFailed, ConfigEntryNotReady
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers.event import async_call_later
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed
from homeassistant.util import dt as dt_util

from .const import (
    COMMAND_STATE_MUTE_SECONDS,
    COMMAND_TIMEOUT,
    CONTROL_STATE_FIELDS,
    DATA_STATE_RECOVERY,
    DEVICE_LIST_QUERY_TIMEOUT,
    DEVICE_LIST_UPDATE_INTERVAL,
    DOMAIN,
    STATE_DATA_MAX_AGE,
    STATE_QUERY_FAILURES_BEFORE_RECOVERY,
    STATE_QUERY_TIMEOUT,
    STATE_RECOVERY_COOLDOWN,
    STATE_RECOVERY_DELAY,
    is_dehumidifier_product_type,
)
from .http import retry_after_from_exception
from .subentries import configured_device_ids

_LOGGER = logging.getLogger(__name__)
_MEASUREMENT_FIELDS = ("environment_humidity", "environment_temperature")


@contextmanager
def _track_request(requests: set[asyncio.Task[object]]) -> Generator[None]:
    """Track nested operations without losing their outer request ownership."""
    task = asyncio.current_task()
    if task is None or task in requests:
        yield
        return
    requests.add(task)
    try:
        yield
    finally:
        requests.discard(task)


async def _async_cancel_requests(requests: set[asyncio.Task[object]]) -> None:
    """Stop network work before its shared MQTT pool is disconnected."""
    current = asyncio.current_task()
    pending = [task for task in requests if task is not current and not task.done()]
    for task in pending:
        task.cancel()
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)


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
        self.last_measurement_received: dict[str, datetime | None] = dict.fromkeys(
            _MEASUREMENT_FIELDS
        )
        self._measurement_deadlines: dict[str, float] = {}
        self._measurement_expiry_unsub: CALLBACK_TYPE | None = None
        self.state_query_failures = 0
        self.state_updates_stalled = False
        self.state_data_stale = False
        self._state_generation = 0
        self._mqtt_measurement_generations: dict[str, int] = {}
        self._state_query_unsub: CALLBACK_TYPE | None = None
        self._recovery_unsub: CALLBACK_TYPE | None = None
        self._stale_unsub: CALLBACK_TYPE | None = None
        self._command_lock = asyncio.Lock()
        self._pending_controls: dict[str, object] = {}
        self._protected_controls: dict[str, object] = {}
        self._active_requests: set[asyncio.Task[object]] = set()

    @override
    async def _async_setup(self) -> None:
        """Track initial connection work so unload can cancel it safely."""
        with _track_request(self._active_requests):
            await self._async_setup_device()

    async def _async_setup_device(self) -> None:
        """Set up the coordinator and subscribe to MQTT state changes."""
        await self.device.ensure_connected()
        if self._shutdown_requested:
            return
        reported_state = self.device.reported_state
        available = self.device.available
        self.data = DeyeDeviceData(
            reported_state=reported_state,
            state=reported_state.copy(),
            available=available,
        )
        self._log_availability(available)
        self._arm_stale_deadline()
        deadline = self.hass.loop.time() + STATE_DATA_MAX_AGE
        self._measurement_deadlines = dict.fromkeys(_MEASUREMENT_FIELDS, deadline)
        self._schedule_measurement_expiry()
        self._unsubscribers.append(
            self.device.subscribe(
                on_state=self._update_mqtt_state,
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
        if self._stale_unsub is not None:
            self._stale_unsub()
            self._stale_unsub = None
        if self._measurement_expiry_unsub is not None:
            self._measurement_expiry_unsub()
            self._measurement_expiry_unsub = None
        self._pending_controls.clear()
        self._protected_controls.clear()

    @override
    async def async_shutdown(self) -> None:
        """Unsubscribe MQTT callbacks and shut down the coordinator."""
        self.unsubscribe()
        await super().async_shutdown()
        await _async_cancel_requests(self._active_requests)
        # An interrupted DataUpdateCoordinator refresh can reschedule itself
        # in its finally block. Remove that timer after cancellation completes.
        await super().async_shutdown()

    def mute_state_update_for_a_while(self) -> None:
        """Protect requested control fields without pausing state reception."""
        if self._shutdown_requested:
            return
        for field in CONTROL_STATE_FIELDS:
            desired = getattr(self.data.state, field)
            if desired is not None and (
                desired != getattr(self.data.reported_state, field)
                or field in self._pending_controls
            ):
                self._pending_controls[field] = desired
        if not self._pending_controls:
            return
        if self.state_update_muted:
            return

        @callback
        def unmute(now: datetime) -> None:
            self.state_update_muted = None
            self._protected_controls.clear()
            self.data = self.data._replace(
                state=self._desired_state(self.data.reported_state)
            )
            self.async_update_listeners()

        self.state_update_muted = async_call_later(
            self.hass, COMMAND_STATE_MUTE_SECONDS, unmute
        )

    def _desired_state(self, reported: DeyeDeviceState) -> DeyeDeviceState:
        """Merge only pending controls into the latest complete telemetry."""
        desired = reported.copy()
        for field, value in (self._protected_controls | self._pending_controls).items():
            setattr(desired, field, value)
        return desired

    @callback
    def _arm_stale_deadline(self) -> None:
        """Only real state reception renews the lifetime of cached data."""
        if self._shutdown_requested:
            return
        if self._stale_unsub is not None:
            self._stale_unsub()
        self._stale_unsub = async_call_later(
            self.hass, STATE_DATA_MAX_AGE, self._state_data_expired
        )

    @callback
    def _state_data_expired(self, _now: datetime) -> None:
        """Hide stale readings without discarding their last known values."""
        self._stale_unsub = None
        if self._shutdown_requested:
            return
        self.state_data_stale = True
        if self.device.available:
            _LOGGER.warning(
                "No state received from %s for %s seconds; cached data is stale",
                self.name,
                STATE_DATA_MAX_AGE,
            )
        self.update_device_availability(self.device.available)

    def measurement_is_stale(self, field: str) -> bool:
        """A partial report must not renew another measurement's cached reading."""
        return self.hass.loop.time() >= self._measurement_deadlines.get(field, 0)

    @callback
    def _schedule_measurement_expiry(self) -> None:
        """Notify entities when their next individual reading expires."""
        if self._shutdown_requested:
            return
        if self._measurement_expiry_unsub is not None:
            self._measurement_expiry_unsub()
            self._measurement_expiry_unsub = None
        now = self.hass.loop.time()
        deadlines = [
            value for value in self._measurement_deadlines.values() if value > now
        ]
        if deadlines:
            self._measurement_expiry_unsub = async_call_later(
                self.hass, min(deadlines) - now, self._measurements_expired
            )

    @callback
    def _measurements_expired(self, _now: datetime) -> None:
        """Expire only missing readings while continuing to expose fresh readings."""
        self._measurement_expiry_unsub = None
        if self._shutdown_requested:
            return
        self.async_update_listeners()
        self._schedule_measurement_expiry()

    def _fog_measured_state(
        self, state: DeyeDeviceState
    ) -> tuple[DeyeDeviceState, set[str]] | None:
        """Keep real measurements and preserve fields absent from a Fog payload."""
        mqtt = self.device._mqtt  # noqa: SLF001
        if not isinstance(mqtt, DeyeFogMqttClient):
            return state, {"environment_humidity", "environment_temperature"}
        # libdeye 3.0.4 records the raw properties immediately before invoking
        # a state callback or returning an HTTP snapshot. DeyeDeviceState alone
        # loses key presence by filling omitted measurements with 27.
        properties = mqtt._fog_last_properties.get(self.device.device_id, {})  # noqa: SLF001
        measured = state.copy()
        received: set[str] = set()
        for key, field in (
            ("CurrentEnvironmentalHumidity", "environment_humidity"),
            ("CurrentAmbientTemperature", "environment_temperature"),
        ):
            value = properties.get(key)
            if value is not None and not isinstance(value, bool):
                try:
                    if isfinite(float(value)):
                        received.add(field)
                        continue
                except TypeError, ValueError, OverflowError:
                    pass
            setattr(measured, field, getattr(self.data.reported_state, field))
        return (measured, received) if received else None

    def _fog_http_state(
        self, state: DeyeDeviceState, generation: int
    ) -> tuple[DeyeDeviceState, set[str]] | None:
        """Accept HTTP readings without losing newer MQTT fields or other readings."""
        measured = self._fog_measured_state(state)
        if measured is None:
            self._restore_device_state()
            if generation != self._state_generation:
                return None
            raise UpdateFailed("Fog properties contain no environmental measurements")
        state, fields = measured
        if generation == self._state_generation:
            return state, fields
        # A partial MQTT update cannot invalidate unrelated HTTP readings.
        # Preserve settings from the newer push while accepting only readings
        # that were not themselves superseded during this HTTP request.
        fields = {
            field
            for field in fields
            if self._mqtt_measurement_generations.get(field, 0) <= generation
        }
        if not fields:
            self._restore_device_state()
            return None
        merged = self.data.reported_state.copy()
        for field in fields:
            setattr(merged, field, getattr(state, field))
        return merged, fields

    def _restore_device_state(self) -> None:
        """Undo SDK mutations from rejected or out-of-order properties."""
        self.device.reported_state = self.data.reported_state
        self.device.state = self.data.state.copy()

    def _update_mqtt_state(self, state: DeyeDeviceState) -> None:
        """Only measured Fog state can supersede the periodic HTTP query."""
        if self._shutdown_requested:
            return
        fields: set[str] | None = None
        if self.device.transport is DeyeDeviceTransport.FOG:
            measured = self._fog_measured_state(state)
            if measured is None:
                self._restore_device_state()
                return
            state, fields = measured
            for field in fields:
                self._mqtt_measurement_generations[field] = self._state_generation + 1
        self.update_device_state(state, measurement_fields=fields)
        if self.device.transport is DeyeDeviceTransport.FOG:
            self._restore_device_state()

    def update_device_state(
        self,
        state: DeyeDeviceState,
        *,
        available: bool = True,
        measurement_fields: set[str] | None = None,
    ) -> None:
        """Will be called when received new DeyeDeviceState."""
        if self._shutdown_requested:
            return
        self._state_generation += 1
        self.last_state_received = dt_util.utcnow()
        fields = (
            _MEASUREMENT_FIELDS if measurement_fields is None else measurement_fields
        )
        deadline = self.hass.loop.time() + STATE_DATA_MAX_AGE
        for field in fields:
            self.last_measurement_received[field] = self.last_state_received
            self._measurement_deadlines[field] = deadline
        self._schedule_measurement_expiry()
        if self.state_query_failures:
            _LOGGER.info("State messages recovered for %s", self.name)
        self.state_query_failures = 0
        self.state_updates_stalled = False
        self.state_data_stale = False
        self._arm_stale_deadline()
        self._cancel_state_query()
        self._cancel_recovery()
        # A device can resume reporting without another MQTT online event.
        self.device.available = available
        self._log_availability(available)
        for field, value in list(self._protected_controls.items()):
            if getattr(state, field) == value:
                self._protected_controls.pop(field)
        self.data = DeyeDeviceData(
            reported_state=state,
            state=self._desired_state(state),
            available=available,
        )
        self.last_update_success = True
        self.async_update_listeners()

    def update_device_availability(self, available: bool) -> None:
        """Will be called when received device availability change."""
        if self._shutdown_requested:
            return
        self.device.available = available
        if not available:
            self._cancel_state_query()
            self._cancel_recovery()
            self.state_query_failures = 0
        available = (
            available and not self.state_updates_stalled and not self.state_data_stale
        )
        self._log_availability(available)
        if available != self.data.available:
            self.data = self.data._replace(available=available)
            self.async_update_listeners()

    @callback
    def sync_cloud_device(self, device: DeyeDevice) -> None:
        """Reconcile online status while keeping the subscribed device alive.

        The cloud list creates new wrappers on each poll. Copy their metadata
        onto the existing wrapper, but keep its MQTT subscriptions and state.
        This also repairs a missed online/offline MQTT notification.
        """
        if self._shutdown_requested:
            return
        self.device.info = device.info
        available = bool(device.info.get("online", self.device.available))
        if (
            available != self.device.available
            or (
                available
                and not self.state_updates_stalled
                and not self.state_data_stale
            )
            != self.data.available
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
        if self._shutdown_requested or not self.device.available:
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
            self._shutdown_requested
            or self.config_entry is None
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

    def sync_reported_state_after_publish(
        self, sent: DeyeDeviceState, baseline: DeyeDeviceState
    ) -> None:
        """Advance the command baseline only for settings that were sent."""
        if self._shutdown_requested:
            return
        reported = self.data.reported_state.copy()
        for field in CONTROL_STATE_FIELDS:
            value = getattr(sent, field)
            if value is not None and value != getattr(baseline, field):
                setattr(reported, field, value)
                if self.state_update_muted:
                    self._protected_controls[field] = value
            if (
                field in self._pending_controls
                and self._pending_controls[field] == value
            ):
                self._pending_controls.pop(field)
        self.data = DeyeDeviceData(
            reported_state=reported,
            state=self._desired_state(reported),
            available=self.data.available,
        )
        self.async_update_listeners()

    def _discard_pending_command(self, sent: DeyeDeviceState) -> None:
        """Discard failed settings while retaining newer user requests."""
        for field, value in list(self._pending_controls.items()):
            if getattr(sent, field) == value:
                self._pending_controls.pop(field)
        self.data = self.data._replace(
            state=self._desired_state(self.data.reported_state)
        )
        self.async_update_listeners()

    @property
    def has_pending_commands(self) -> bool:
        """Return whether newer requests still need sending on this device."""
        return bool(self._pending_controls) and not self._shutdown_requested

    async def async_publish_command(self) -> None:
        """Track commands, including those waiting for the device send lock."""
        with _track_request(self._active_requests):
            await self._async_send_pending_commands()

    async def _async_send_pending_commands(self) -> None:
        """Serialize a device's commands and drain requests received while sending."""
        async with self._command_lock:
            while not self._shutdown_requested:
                sent = self.data.state.copy()
                baseline = self.data.reported_state.copy()
                command = sent.to_command()
                if command == baseline.to_command():
                    self._discard_pending_command(sent)
                    return
                try:
                    async with asyncio.timeout(COMMAND_TIMEOUT):
                        await self.device.apply(command, baseline=baseline)
                except Exception, asyncio.CancelledError:
                    if not self._shutdown_requested:
                        self._discard_pending_command(sent)
                    raise
                self.sync_reported_state_after_publish(sent, baseline)
                if not self._pending_controls:
                    return

    @override
    async def _async_update_data(self) -> DeyeDeviceData:
        """Track the refresh until its transport and callbacks have finished."""
        with _track_request(self._active_requests):
            return await self._async_fetch_device_state()

    async def _async_fetch_device_state(self) -> DeyeDeviceData:
        """Poll device state. Some Deye devices have a long heartbeat period.

        Fog requests a RealData report and reads properties over HTTP without
        requiring an MQTT reply. Classic/Combo query and await MQTT state.
        """
        generation = self._state_generation
        reported_state: DeyeDeviceState | None
        try:
            async with asyncio.timeout(STATE_QUERY_TIMEOUT):
                if self.device.transport is DeyeDeviceTransport.FOG:
                    await self.device.request_refresh()
                    if self._shutdown_requested:
                        return self.data
                    generation = self._state_generation
                    reported_state = await self.device.refresh()
                else:
                    reported_state = await self.device.request_refresh()
        except DeyeCloudApiInvalidAuthError as err:
            raise ConfigEntryAuthFailed from err
        except (DeyeCloudApiCannotConnectError, OSError) as err:
            raise UpdateFailed(
                f"Error communicating with {self.name}: {str(err) or type(err).__name__}",
                retry_after=retry_after_from_exception(err),
            ) from err

        if self._shutdown_requested:
            return self.data
        if reported_state is None:
            if generation == self._state_generation and self._state_query_unsub is None:
                self._state_query_unsub = async_call_later(
                    self.hass, STATE_QUERY_TIMEOUT, self._state_query_expired
                )
            return self.data
        measurement_fields: set[str] | None = None
        if self.device.transport is DeyeDeviceTransport.FOG:
            measured = self._fog_http_state(reported_state, generation)
            if measured is None:
                return self.data
            reported_state, measurement_fields = measured
        elif generation != self._state_generation:
            # A pushed state arrived while HTTP was pending. Its device wrapper
            # was overwritten by refresh() just before returning the old result.
            self._restore_device_state()
            return self.data
        self.update_device_state(
            reported_state,
            measurement_fields=measurement_fields,
            available=(
                self.device.available
                if self.device.transport is DeyeDeviceTransport.FOG
                else True
            ),
        )
        if self.device.transport is DeyeDeviceTransport.FOG:
            self._restore_device_state()
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
        self._device_sync_lock = asyncio.Lock()
        self._active_requests: set[asyncio.Task[object]] = set()

    @override
    async def async_shutdown(self) -> None:
        """Cancel discovery before the integration disconnects pooled clients."""
        await super().async_shutdown()
        await _async_cancel_requests(self._active_requests)
        await super().async_shutdown()

    @override
    async def _async_update_data(self) -> list[DeyeApiResponseDeviceInfo]:
        """Track the account refresh, including any device setup it starts."""
        with _track_request(self._active_requests):
            return await self._async_fetch_devices()

    async def _async_fetch_devices(self) -> list[DeyeApiResponseDeviceInfo]:
        """Fetch the cloud device list and add or remove local devices."""
        try:
            async with asyncio.timeout(DEVICE_LIST_QUERY_TIMEOUT):
                devices = [
                    device
                    for device in await self._client.list_devices()
                    if is_dehumidifier_product_type(device.info["product_type"])
                ]
        except DeyeCloudApiInvalidAuthError as err:
            raise ConfigEntryAuthFailed from err
        except (DeyeCloudApiCannotConnectError, OSError) as err:
            raise UpdateFailed(
                "Cannot connect to Deye Cloud",
                retry_after=retry_after_from_exception(err),
            ) from err

        if self._shutdown_requested:
            return list(self.device_list)
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
        with _track_request(self._active_requests):
            async with self._device_sync_lock:
                if not self._shutdown_requested:
                    await self._async_sync_active_devices(devices)

    async def _async_sync_active_devices(self, devices: list[DeyeDevice]) -> None:
        """Apply a device list while holding the synchronization lock."""
        configured = (
            configured_device_ids(self.config_entry)
            if self.config_entry is not None
            else set()
        )
        current_ids = {device.device_id for device in devices}

        for device_id in list(self.coordinator_map):
            if self._shutdown_requested:
                return
            if device_id not in current_ids:
                await self._async_remove_stale_device(device_id)
            elif device_id not in configured:
                coordinator = self.coordinator_map.pop(device_id, None)
                if coordinator is not None:
                    await coordinator.async_shutdown()

        for device in devices:
            if self._shutdown_requested:
                return
            if coordinator := self.coordinator_map.get(device.device_id):
                coordinator.sync_cloud_device(device)
            elif device.device_id in configured:
                await self._async_add_device(device)

        if self._shutdown_requested:
            return
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
        except ConfigEntryAuthFailed, asyncio.CancelledError:
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
        if self._shutdown_requested:
            await coordinator.async_shutdown()
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
        if self._shutdown_requested or mac is None or self.config_entry is None:
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
