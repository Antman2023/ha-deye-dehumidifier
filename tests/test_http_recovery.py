"""Exercise real SDK error handling against a local HTTP server."""

import asyncio
from collections.abc import AsyncGenerator, Generator
from dataclasses import dataclass, field
from datetime import timedelta
from email.utils import format_datetime
from unittest.mock import MagicMock, patch

from aiohttp import ClientResponseError, web
from aiohttp.test_utils import TestServer
from freezegun.api import FrozenDateTimeFactory
import jwt
from libdeye.client import DeyeClient, DeyeDevice
from libdeye.cloud_api import (
    DeyeCloudApiCannotConnectError,
    DeyeCloudApiInvalidAuthError,
    DeyeIotPlatform,
)
from libdeye.mqtt_client import DeyeFogMqttClient
import pytest
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.deye_dehumidifier import async_remove_entry
from custom_components.deye_dehumidifier.cloud_api import DeyeCloudApi
from custom_components.deye_dehumidifier.const import DATA_HTTP_COOLDOWNS, DOMAIN
from custom_components.deye_dehumidifier.data_coordinator import (
    DeyeDataUpdateCoordinator,
)
from custom_components.deye_dehumidifier.http import (
    async_create_deye_session,
    retry_after_from_exception,
)
from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import UpdateFailed
from homeassistant.util import dt as dt_util
from tests.helpers import MOCK_CONFIG, MOCK_DEVICE_INFO
from tests.test_state_recovery import _advance

pytestmark = [
    pytest.mark.usefixtures("socket_enabled"),
    pytest.mark.allow_hosts(["127.0.0.1"]),
]


@pytest.fixture
def http_clock() -> Generator[MagicMock]:
    """Advance the account cooldown without changing asyncio's network clock."""
    with patch(
        "custom_components.deye_dehumidifier.http.monotonic", return_value=1000.0
    ) as clock:
        yield clock


def _token(lifetime: int = 604800) -> str:
    """Create a local-only token accepted by the SDK's unverified JWT decoder."""
    return jwt.encode(
        {
            "enduserid": "local-test",
            "exp": int(dt_util.utcnow().timestamp()) + lifetime,
        },
        "",
        algorithm="none",
    )


@dataclass
class _Cloud:
    """Return controlled envelopes without contacting the Deye cloud."""

    errors: dict[str, tuple[int, int]] = field(default_factory=dict)
    requests: list[str] = field(default_factory=list)
    current_humidity: int = 63
    reported_humidity: int = 63
    retry_after: str = "120"
    gates: dict[str, tuple[asyncio.Event, asyncio.Event]] = field(default_factory=dict)

    async def respond(self, request: web.Request) -> web.Response:
        """Return a properties snapshot, credentials, or a chosen HTTP error."""
        self.requests.append(request.path)
        error = self.errors.get(request.path)
        if gate := self.gates.get(request.query.get("device_id", "")):
            entered, finish = gate
            entered.set()
            await finish.wait()
        if error:
            status, code = error
            return web.json_response(
                {"meta": {"code": code, "message": "request failed"}, "data": {}},
                status=status,
                headers={"Retry-After": self.retry_after},
            )
        data: dict[str, object]
        if request.path == "/set/properties/":
            body = await request.json()
            if body.get("params", {}).get("RealData") == 1:
                self.reported_humidity = self.current_humidity
            data = {}
        elif request.path in {"/login/", "/refreshToken/"}:
            data = {"token": _token()}
        else:
            data = {
                "properties": {"CurrentEnvironmentalHumidity": self.reported_humidity}
            }
        return web.json_response({"meta": {"code": 0, "message": "OK"}, "data": data})


@pytest.fixture
async def http_cloud() -> AsyncGenerator[_Cloud]:
    """Redirect all SDK HTTP requests to a loopback-only server."""
    cloud = _Cloud()
    app = web.Application()
    app.router.add_route("*", "/{path:.*}", cloud.respond)
    server = TestServer(app, host="127.0.0.1")
    await server.start_server(access_log=None)
    try:
        with patch(
            "libdeye.cloud_api.DEYE_API_END_USER_ENDPOINT",
            str(server.make_url("/")).rstrip("/"),
        ):
            yield cloud
    finally:
        await server.close()


@pytest.mark.parametrize("status", [408, 429, 500, 503])
async def test_transient_json_error_keeps_polling_and_recovers(
    hass: HomeAssistant, http_cloud: _Cloud, http_clock: MagicMock, status: int
) -> None:
    """An HTTP timeout, rate limit or outage must not stop polling as an auth failure."""
    api = DeyeCloudApi(
        async_create_deye_session(hass), "local-user", "local-pass", _token()
    )
    client = DeyeClient(api)
    client._mqtt_by_type[DeyeFogMqttClient] = DeyeFogMqttClient(api)
    device = DeyeDevice(client, MOCK_DEVICE_INFO | {"platform": DeyeIotPlatform.Fog})
    entry = MockConfigEntry(domain=DOMAIN, data=MOCK_CONFIG, version=2)
    entry.add_to_hass(hass)
    coordinator = DeyeDataUpdateCoordinator(hass, entry, device)
    await coordinator._async_setup()
    remove = coordinator.async_add_listener(lambda: None)
    try:
        http_cloud.errors["/get/properties/"] = (status, status)
        await coordinator.async_refresh()
        assert not coordinator.last_update_success
        assert isinstance(coordinator.last_exception, UpdateFailed)
        assert coordinator._unsub_refresh is not None
        assert coordinator.last_exception.retry_after == (
            120 if status == 429 else None
        )
        http_cloud.errors.clear()
        if status == 429:
            requests = len(http_cloud.requests)
            await coordinator.async_refresh()
            assert len(http_cloud.requests) == requests
            http_clock.return_value += 121
        await coordinator.async_refresh()
        assert coordinator.last_update_success
        assert coordinator.data.state.environment_humidity == 63
        assert "/login/" not in http_cloud.requests
    finally:
        remove()
        await coordinator.async_shutdown()


@pytest.mark.parametrize("status", [408, 503])
async def test_token_refresh_service_error_does_not_trigger_login(
    hass: HomeAssistant, http_cloud: _Cloud, status: int
) -> None:
    """A temporary refreshToken outage must not masquerade as invalid credentials."""
    api = DeyeCloudApi(
        async_create_deye_session(hass), "local-user", "local-pass", _token(3600)
    )
    http_cloud.errors["/refreshToken/"] = (status, status)
    with pytest.raises(DeyeCloudApiCannotConnectError):
        await api.get_fog_platform_device_properties("device")
    assert http_cloud.requests == ["/refreshToken/"]
    http_cloud.errors.clear()
    state = await api.get_fog_platform_device_properties("device")
    assert state["CurrentEnvironmentalHumidity"] == 63
    assert http_cloud.requests == [
        "/refreshToken/",
        "/refreshToken/",
        "/get/properties/",
    ]


@pytest.mark.parametrize("renewal_rejected", [False, True])
async def test_concurrent_polls_refresh_expiring_token_once(
    hass: HomeAssistant, http_cloud: _Cloud, renewal_rejected: bool
) -> None:
    """Devices waking together must not all refresh the same expiring token."""
    api = DeyeCloudApi(
        async_create_deye_session(hass), "local-user", "local-pass", _token(3600)
    )
    refreshed = MagicMock()
    api.on_auth_token_refreshed = refreshed
    if renewal_rejected:
        http_cloud.errors["/refreshToken/"] = (200, 401)
    states = await asyncio.gather(
        *(
            api.get_fog_platform_device_properties(f"device-{index}")
            for index in range(6)
        )
    )
    assert all(state["CurrentEnvironmentalHumidity"] == 63 for state in states)
    assert http_cloud.requests.count("/refreshToken/") == 1
    assert http_cloud.requests.count("/get/properties/") == 6
    assert http_cloud.requests.count("/login/") == int(renewal_rejected)
    refreshed.assert_called_once_with(api.auth_token)


@pytest.mark.parametrize("cancel_owner", [False, True])
async def test_cancelling_token_renewal_cannot_leave_refresh_locked(
    hass: HomeAssistant, http_cloud: _Cloud, cancel_owner: bool
) -> None:
    """Cancellation of the lock owner or a waiter cannot stop future device polls."""
    api = DeyeCloudApi(
        async_create_deye_session(hass), "local-user", "local-pass", _token(3600)
    )
    entered, finish = asyncio.Event(), asyncio.Event()
    http_cloud.gates[""] = entered, finish
    owner = hass.async_create_task(api.get_fog_platform_device_properties("owner"))
    waiter = None
    try:
        async with asyncio.timeout(5):
            await entered.wait()
        waiter = hass.async_create_task(
            api.get_fog_platform_device_properties("waiter")
        )
        await asyncio.sleep(0)
        cancelled, survivor = (owner, waiter) if cancel_owner else (waiter, owner)
        cancelled.cancel()
        with pytest.raises(asyncio.CancelledError):
            await cancelled
        assert not survivor.done()
        http_cloud.gates.clear()
        finish.set()
        async with asyncio.timeout(5):
            assert (await survivor)["CurrentEnvironmentalHumidity"] == 63
            assert (await api.get_fog_platform_device_properties("next"))[
                "CurrentEnvironmentalHumidity"
            ] == 63
        assert http_cloud.requests.count("/refreshToken/") == (2 if cancel_owner else 1)
        assert "/login/" not in http_cloud.requests
    finally:
        finish.set()
        tasks = [owner] if waiter is None else [owner, waiter]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def test_fog_requests_new_device_report_before_reading_cloud_snapshot(
    hass: HomeAssistant, http_cloud: _Cloud
) -> None:
    """Refresh a cloud snapshot even when the HA MQTT subscriber receives nothing."""
    api = DeyeCloudApi(
        async_create_deye_session(hass), "local-user", "local-pass", _token()
    )
    http_cloud.reported_humidity = 47
    http_cloud.current_humidity = 64
    assert (await api.get_fog_platform_device_properties("device"))[
        "CurrentEnvironmentalHumidity"
    ] == 47
    client = DeyeClient(api)
    client._mqtt_by_type[DeyeFogMqttClient] = DeyeFogMqttClient(api)
    device = DeyeDevice(client, MOCK_DEVICE_INFO | {"platform": DeyeIotPlatform.Fog})
    entry = MockConfigEntry(domain=DOMAIN, data=MOCK_CONFIG, version=2)
    entry.add_to_hass(hass)
    coordinator = DeyeDataUpdateCoordinator(hass, entry, device)
    try:
        await coordinator._async_setup()
        await coordinator.async_refresh()
        assert coordinator.data.state.environment_humidity == 64
        assert coordinator.data.available
        assert http_cloud.requests[-2:] == ["/set/properties/", "/get/properties/"]
        assert coordinator._state_query_unsub is None
    finally:
        await coordinator.async_shutdown()


@pytest.mark.parametrize("status", [200, 401, 403])
async def test_real_auth_rejection_remains_auth_error(
    hass: HomeAssistant, http_cloud: _Cloud, status: int
) -> None:
    """Preserve the SDK's existing handling of invalid-auth envelopes."""
    api = DeyeCloudApi(
        async_create_deye_session(hass), "local-user", "local-pass", _token()
    )
    http_cloud.errors["/get/properties/"] = (status, 401)
    with pytest.raises(DeyeCloudApiInvalidAuthError):
        await api.get_fog_platform_device_properties("device")


@pytest.mark.parametrize("status", [408, 429, 503])
async def test_transient_login_error_is_not_reported_as_bad_password(
    hass: HomeAssistant, http_cloud: _Cloud, status: int
) -> None:
    """Do not direct users to change credentials because of a transient HTTP error."""
    api = DeyeCloudApi(async_create_deye_session(hass), "local-user", "local-pass")
    http_cloud.errors["/login/"] = (status, status)
    with pytest.raises(DeyeCloudApiCannotConnectError) as raised:
        await api.authenticate()
    assert retry_after_from_exception(raised.value) == (120 if status == 429 else None)


@pytest.mark.parametrize("retry_after", ["nan", "inf", "-inf", "1e9999", "invalid"])
async def test_invalid_retry_after_cannot_stop_scheduling(retry_after: str) -> None:
    """Non-finite or malformed Retry-After values fall back to a finite delay."""
    error = ClientResponseError(
        request_info=MagicMock(),
        history=(),
        status=429,
        headers={"Retry-After": retry_after},
    )
    assert retry_after_from_exception(error) == 60


async def test_http_date_retry_after_is_respected(
    freezer: FrozenDateTimeFactory,
) -> None:
    """Retry-After also accepts a date instead of seconds."""
    error = ClientResponseError(
        request_info=MagicMock(),
        history=(),
        status=429,
        headers={
            "Retry-After": format_datetime(dt_util.utcnow() + timedelta(seconds=120))
        },
    )
    assert retry_after_from_exception(error) == pytest.approx(120, abs=1)


async def test_cooldown_applies_to_all_operations_on_one_account(
    hass: HomeAssistant, http_cloud: _Cloud, http_clock: MagicMock
) -> None:
    """A device, command and MQTT reconnect must honor the same Retry-After."""
    session = async_create_deye_session(hass, "account-a")
    api = DeyeCloudApi(session, "account-a", "local-pass", _token())
    http_cloud.errors["/get/properties/"] = (429, 429)
    with pytest.raises(DeyeCloudApiCannotConnectError):
        await api.get_fog_platform_device_properties("device-1")
    http_cloud.errors.clear()
    for operation in (
        api.get_fog_platform_device_properties("device-2"),
        api.set_fog_platform_device_properties("device-2", {"Power": 0}),
        api.get_fog_platform_mqtt_info(),
    ):
        with pytest.raises(DeyeCloudApiCannotConnectError) as raised:
            await operation
        assert retry_after_from_exception(raised.value) == 120
    assert http_cloud.requests == ["/get/properties/"]
    http_clock.return_value += 121
    assert (await api.get_fog_platform_device_properties("device-1"))[
        "CurrentEnvironmentalHumidity"
    ] == 63
    assert http_cloud.requests == ["/get/properties/", "/get/properties/"]
    assert "account-a" not in hass.data[DATA_HTTP_COOLDOWNS]


async def test_recreated_sessions_share_account_cooldown_without_blocking_others(
    hass: HomeAssistant, http_cloud: _Cloud, http_clock: MagicMock
) -> None:
    """Reload and UI sessions cannot bypass a cooldown on the same account."""
    api = DeyeCloudApi(
        async_create_deye_session(hass, "account-a"),
        "account-a",
        "local-pass",
        _token(),
    )
    http_cloud.errors["/get/properties/"] = (429, 429)
    with pytest.raises(DeyeCloudApiCannotConnectError):
        await api.get_fog_platform_device_properties("device")
    http_cloud.errors.clear()
    replacement = DeyeCloudApi(
        async_create_deye_session(hass, "account-a"),
        "account-a",
        "local-pass",
        _token(),
    )
    with pytest.raises(DeyeCloudApiCannotConnectError):
        await replacement.get_fog_platform_device_properties("device")
    assert len(http_cloud.requests) == 1
    other = DeyeCloudApi(
        async_create_deye_session(hass, "account-b"),
        "account-b",
        "local-pass",
        _token(),
    )
    assert (await other.get_fog_platform_device_properties("other-device"))[
        "CurrentEnvironmentalHumidity"
    ] == 63
    assert len(http_cloud.requests) == 2
    http_clock.return_value += 121
    await replacement.get_fog_platform_device_properties("device")
    assert len(http_cloud.requests) == 3


async def test_zero_retry_after_cannot_spin_requests(
    hass: HomeAssistant, http_cloud: _Cloud, http_clock: MagicMock
) -> None:
    """A zero Retry-After still paces consecutive failed requests."""
    api = DeyeCloudApi(async_create_deye_session(hass), "local", "local-pass", _token())
    http_cloud.retry_after = "0"
    http_cloud.errors["/get/properties/"] = (429, 429)
    with pytest.raises(DeyeCloudApiCannotConnectError):
        await api.get_fog_platform_device_properties("device")
    http_cloud.errors.clear()
    with pytest.raises(DeyeCloudApiCannotConnectError) as raised:
        await api.get_fog_platform_device_properties("device")
    assert retry_after_from_exception(raised.value) == 1
    assert len(http_cloud.requests) == 1
    http_clock.return_value += 1.01
    await api.get_fog_platform_device_properties("device")
    assert len(http_cloud.requests) == 2


async def test_late_success_response_cannot_cancel_newer_rate_limit(
    hass: HomeAssistant, http_cloud: _Cloud, http_clock: MagicMock
) -> None:
    """An already-pending successful request cannot clear another request's 429."""
    api = DeyeCloudApi(async_create_deye_session(hass), "local", "local-pass", _token())
    entered, finish = asyncio.Event(), asyncio.Event()
    http_cloud.gates["slow"] = entered, finish
    slow = hass.async_create_task(api.get_fog_platform_device_properties("slow"))
    try:
        await entered.wait()
        http_cloud.errors["/get/properties/"] = (429, 429)
        with pytest.raises(DeyeCloudApiCannotConnectError):
            await api.get_fog_platform_device_properties("limited")
        finish.set()
        await slow
        http_cloud.errors.clear()
        with pytest.raises(DeyeCloudApiCannotConnectError):
            await api.get_fog_platform_device_properties("next")
        assert len(http_cloud.requests) == 2
    finally:
        finish.set()
        await slow


async def test_removing_account_discards_only_its_cooldown(hass: HomeAssistant) -> None:
    """Permanent removal frees its account state; other accounts retain backoff."""
    hass.data[DATA_HTTP_COOLDOWNS] = {MOCK_CONFIG["username"]: 1000.0, "other": 2000.0}
    entry = MockConfigEntry(domain=DOMAIN, data=MOCK_CONFIG, version=2)
    await async_remove_entry(hass, entry)
    assert hass.data[DATA_HTTP_COOLDOWNS] == {"other": 2000.0}


@pytest.mark.parametrize("outage_status", [408, 503])
async def test_scheduled_http_polls_recover_after_rate_limit_and_stale_data(
    hass: HomeAssistant,
    http_cloud: _Cloud,
    http_clock: MagicMock,
    freezer: FrozenDateTimeFactory,
    outage_status: int,
) -> None:
    """HA timers alone must recover after an outage and keep stable values fresh."""
    api = DeyeCloudApi(
        async_create_deye_session(hass, "account"),
        "account",
        "local-pass",
        _token(3600),
    )
    client = DeyeClient(api)
    client._mqtt_by_type[DeyeFogMqttClient] = DeyeFogMqttClient(api)
    device = DeyeDevice(client, MOCK_DEVICE_INFO | {"platform": DeyeIotPlatform.Fog})
    entry = MockConfigEntry(domain=DOMAIN, data=MOCK_CONFIG, version=2)
    entry.add_to_hass(hass)
    coordinator = DeyeDataUpdateCoordinator(hass, entry, device)

    async def advance(seconds: int) -> None:
        http_clock.return_value += seconds
        await _advance(hass, freezer, seconds)
        await hass.async_block_till_done(wait_background_tasks=True)

    await coordinator._async_setup()
    await coordinator.async_refresh()
    last_received = coordinator.last_state_received
    remove = coordinator.async_add_listener(lambda: None)
    try:
        http_cloud.errors["/get/properties/"] = (429, 429)
        await advance(31)
        assert not coordinator.last_update_success
        assert coordinator.data.available
        failed_requests = len(http_cloud.requests)
        await advance(60)
        coordinator.update_device_availability(True)
        await advance(58)
        assert len(http_cloud.requests) == failed_requests
        http_cloud.errors["/get/properties/"] = (outage_status, outage_status)
        await advance(3)
        assert len(http_cloud.requests) > failed_requests
        assert not coordinator.last_update_success
        assert coordinator.data.available
        await advance(31)
        assert coordinator.state_data_stale
        assert not coordinator.data.available
        assert coordinator.last_state_received == last_received
        coordinator.update_device_availability(True)
        assert not coordinator.data.available

        http_cloud.errors.clear()
        http_cloud.current_humidity = 72
        await advance(31)
        assert coordinator.last_update_success
        assert coordinator.data.available
        assert not coordinator.state_data_stale
        assert coordinator.data.state.environment_humidity == 72
        for _ in range(60):
            last_received = coordinator.last_state_received
            coordinator.update_device_availability(True)
            await advance(31)
            assert coordinator.last_state_received > last_received
            assert coordinator.data.state.environment_humidity == 72
            assert coordinator.data.available
            assert not coordinator.state_data_stale
        assert http_cloud.requests.count("/refreshToken/") == 1
        assert "/login/" not in http_cloud.requests
    finally:
        remove()
        await coordinator.async_shutdown()
