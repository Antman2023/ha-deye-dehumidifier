"""Cloud API coordination shared by device polls and MQTT credential refreshes."""

import asyncio
from typing import override

from aiohttp import ClientSession
from libdeye.cloud_api import DeyeCloudApi as LibDeyeCloudApi


class DeyeCloudApi(LibDeyeCloudApi):
    """Serialize token renewal before the SDK rechecks its expiration time."""

    def __init__(
        self,
        session: ClientSession,
        username: str,
        password: str,
        auth_token: str | None = None,
    ) -> None:
        """Initialize the SDK and this client's token renewal lock."""
        super().__init__(session, username, password, auth_token)
        self._token_refresh_lock = asyncio.Lock()

    @override
    async def refresh_token_if_near_expiry(self, force: bool = False) -> None:
        """Let waiting requests reuse a token another request just renewed."""
        async with self._token_refresh_lock:
            await super().refresh_token_if_near_expiry(force=force)
