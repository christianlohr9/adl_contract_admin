"""Async MFL API client with authentication, rate limiting, and retry."""

from __future__ import annotations

import asyncio
import time
from typing import Any, Self
from xml.etree import ElementTree

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from app.mfl.exceptions import MFLAPIError, MFLAuthError, MFLRateLimitError


class MFLClient:
    """Async client for the MFL (MyFantasyLeague) API.

    Supports API key authentication (preferred) with fallback to cookie-based
    authentication. Enforces rate limiting between requests and uses tenacity
    for automatic retries with exponential backoff.

    Usage::

        async with MFLClient.from_settings(settings) as client:
            league_data = await client.league()
    """

    def __init__(
        self,
        year: int,
        league_id: int,
        base_url: str,
        api_key: str = "",
        username: str = "",
        password: str = "",
        request_delay: float = 1.0,
    ) -> None:
        self.year = year
        self.league_id = league_id
        self.base_url = base_url.rstrip("/")
        self._api_key = api_key
        self._username = username
        self._password = password
        self._request_delay = request_delay
        self._cookie: str | None = None
        self._http: httpx.AsyncClient | None = None
        self._last_request_at: float = 0.0

    async def __aenter__(self) -> Self:
        self._http = httpx.AsyncClient(timeout=httpx.Timeout(30.0))
        await self._authenticate()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        if self._http is not None:
            await self._http.aclose()
            self._http = None

    # ── Authentication ──────────────────────────────────────────────

    async def _authenticate(self) -> None:
        """Authenticate with the MFL API.

        Prefers API key authentication. Falls back to cookie-based auth
        by calling the login endpoint and parsing the cookie value from XML.
        """
        if self._api_key:
            return

        if not self._username or not self._password:
            msg = "Either api_key or username/password must be provided"
            raise MFLAuthError(msg)

        assert self._http is not None  # noqa: S101
        url = f"{self.base_url}/{self.year}/login"
        params = {
            "USERNAME": self._username,
            "PASSWORD": self._password,
            "XML": "1",
        }
        resp = await self._http.get(url, params=params)

        if resp.status_code != 200:
            msg = f"Login failed with status {resp.status_code}: {resp.text}"
            raise MFLAuthError(msg)

        try:
            root = ElementTree.fromstring(resp.text)  # noqa: S314
            status = root.get("status", "")
            if status == "OK":
                cookie_value = root.get("MFL_USER_ID", "")
                if not cookie_value:
                    msg = "Login response missing MFL_USER_ID cookie value"
                    raise MFLAuthError(msg)
                self._cookie = cookie_value
            else:
                error_msg = root.get("error", "Unknown login error")
                raise MFLAuthError(error_msg)
        except ElementTree.ParseError as e:
            msg = f"Failed to parse login response XML: {e}"
            raise MFLAuthError(msg) from e

    # ── Core request method ─────────────────────────────────────────

    async def _export(self, type_: str, **params: Any) -> dict[str, Any]:
        """Execute an MFL export API call with rate limiting and retry.

        Args:
            type_: The MFL export TYPE parameter (e.g., "league", "rosters").
            **params: Additional query parameters.

        Returns:
            Parsed JSON response as a dictionary.
        """
        return await self._export_with_retry(type_, **params)

    @retry(
        retry=retry_if_exception_type(httpx.HTTPStatusError),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        stop=stop_after_attempt(5),
        reraise=True,
    )
    async def _export_with_retry(self, type_: str, **params: Any) -> dict[str, Any]:
        """Inner export method wrapped with tenacity retry logic."""
        assert self._http is not None  # noqa: S101

        # Enforce rate limiting
        now = time.monotonic()
        elapsed = now - self._last_request_at
        if elapsed < self._request_delay:
            await asyncio.sleep(self._request_delay - elapsed)

        query_params: dict[str, Any] = {
            "TYPE": type_,
            "L": self.league_id,
            "JSON": "1",
            **params,
        }

        headers: dict[str, str] = {}
        if self._api_key:
            query_params["APIKEY"] = self._api_key
        elif self._cookie:
            headers["Cookie"] = f"MFL_USER_ID={self._cookie}"

        url = f"{self.base_url}/{self.year}/export"
        resp = await self._http.get(url, params=query_params, headers=headers)
        self._last_request_at = time.monotonic()

        if resp.status_code == 429:
            raise MFLRateLimitError("MFL API rate limit exceeded")

        if resp.status_code >= 400:
            raise MFLAPIError(resp.status_code, resp.text)

        result: dict[str, Any] = resp.json()
        return result

    # ── Convenience methods ─────────────────────────────────────────

    async def league(self) -> dict[str, Any]:
        """Fetch league settings and franchise list."""
        return await self._export("league")

    async def rosters(self) -> dict[str, Any]:
        """Fetch all franchise rosters with contract info."""
        return await self._export("rosters")

    async def players(self) -> dict[str, Any]:
        """Fetch player details (all or filtered)."""
        return await self._export("players", DETAILS="1")

    async def player_scores(
        self, week: str = "YTD", year: int | None = None
    ) -> dict[str, Any]:
        """Fetch player scoring data."""
        params: dict[str, Any] = {"W": week}
        if year is not None:
            params["YEAR"] = year
        return await self._export("playerScores", **params)

    async def free_agents(self) -> dict[str, Any]:
        """Fetch available free agents."""
        return await self._export("freeAgents")

    async def salaries(self) -> dict[str, Any]:
        """Fetch salary information for all players."""
        return await self._export("salaries")

    async def transactions(self, **params: Any) -> dict[str, Any]:
        """Fetch league transactions with optional filters."""
        return await self._export("transactions", **params)

    async def league_standings(self) -> dict[str, Any]:
        """Fetch current league standings."""
        return await self._export("leagueStandings")

    # ── Factory ─────────────────────────────────────────────────────

    @classmethod
    def from_settings(cls, settings: Any) -> MFLClient:
        """Create an MFLClient from application Settings.

        Args:
            settings: Application Settings instance with MFL config fields.

        Returns:
            Configured MFLClient (not yet connected — use as async context manager).
        """
        return cls(
            year=settings.mfl_year,
            league_id=settings.mfl_league_id,
            base_url=settings.mfl_base_url,
            api_key=settings.mfl_api_key,
            username=settings.mfl_username,
            password=settings.mfl_password,
            request_delay=settings.mfl_request_delay,
        )
