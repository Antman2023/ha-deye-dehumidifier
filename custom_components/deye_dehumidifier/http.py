"""HTTP status handling and shared account rate-limit cooldowns."""

from collections.abc import Mapping
from datetime import UTC
from email.utils import parsedate_to_datetime
from math import ceil, isfinite
from time import monotonic

from aiohttp import (
    ClientHandlerType,
    ClientRequest,
    ClientResponse,
    ClientResponseError,
    ClientSession,
    ClientTimeout,
)
from multidict import CIMultiDict

from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.aiohttp_client import async_create_clientsession
from homeassistant.util import dt as dt_util

from .const import DATA_HTTP_COOLDOWNS, DEVICE_LIST_QUERY_TIMEOUT

_HTTP_TOO_MANY_REQUESTS = 429
_DEFAULT_RETRY_AFTER = 60.0


def _retry_after_headers(headers: Mapping[str, str] | None) -> float:
    """Return a finite delay from seconds or an HTTP-date Retry-After header."""
    value = (
        None
        if headers is None
        else headers.get("Retry-After") or headers.get("retry-after")
    )
    if value is None:
        return _DEFAULT_RETRY_AFTER
    try:
        seconds = float(value)
    except TypeError, ValueError:
        try:
            deadline = parsedate_to_datetime(value)
            if deadline.tzinfo is None:
                deadline = deadline.replace(tzinfo=UTC)
            seconds = (deadline - dt_util.utcnow()).total_seconds()
        except TypeError, ValueError, OverflowError:
            return _DEFAULT_RETRY_AFTER
    return max(seconds, 0.0) if isfinite(seconds) else _DEFAULT_RETRY_AFTER


def retry_after_from_exception(err: BaseException) -> float | None:
    """Find a rate-limit delay while preserving SDK exception wrapping."""
    seen: set[int] = set()
    current: BaseException | None = err
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        status = getattr(current, "status", None)
        if status is None:
            status = getattr(current, "status_code", None)
        if status == _HTTP_TOO_MANY_REQUESTS:
            return _retry_after_headers(getattr(current, "headers", None))
        current = current.__cause__ or current.__context__
    return None


async def _async_check_response(response: ClientResponse) -> None:
    """Keep timeouts, rate limits and outages out of the invalid-auth parser."""
    if response.status in {408, 429} or response.status >= 500:
        response.raise_for_status()


@callback
def async_create_deye_session(
    hass: HomeAssistant, account: str | None = None
) -> ClientSession:
    """Create an HA-managed session with bounded requests and status handling."""
    # Runtime, setup retries and subentry flows share the cooldown by account.
    # Sessions without an account still share it among their own SDK operations.
    cooldowns: dict[str | None, float] = (
        {} if account is None else hass.data.setdefault(DATA_HTTP_COOLDOWNS, {})
    )
    now = monotonic()
    for key, deadline in list(cooldowns.items()):
        if deadline <= now:
            cooldowns.pop(key)

    async def rate_limit(
        request: ClientRequest, handler: ClientHandlerType
    ) -> ClientResponse:
        remaining = cooldowns.get(account, 0.0) - monotonic()
        if remaining > 0:
            raise ClientResponseError(
                request_info=request.request_info,
                history=(),
                status=_HTTP_TOO_MANY_REQUESTS,
                message="Deye account rate-limit cooldown is active",
                headers=CIMultiDict({"Retry-After": str(max(ceil(remaining), 1))}),
            )
        cooldowns.pop(account, None)
        response = await handler(request)
        if response.status == _HTTP_TOO_MANY_REQUESTS:
            # Pace even a zero/past retry value rather than creating a hot loop.
            deadline = monotonic() + max(_retry_after_headers(response.headers), 1.0)
            cooldowns[account] = max(cooldowns.get(account, 0.0), deadline)
        return response

    return async_create_clientsession(
        hass,
        middlewares=(rate_limit,),
        raise_for_status=_async_check_response,
        timeout=ClientTimeout(total=DEVICE_LIST_QUERY_TIMEOUT),
    )
