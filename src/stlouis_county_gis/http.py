from __future__ import annotations

import asyncio
import random
from typing import Any, Literal

import httpx

from .exceptions import TransportError


HttpMethod = Literal["GET", "POST"]


TRANSIENT_STATUS = {429, 500, 502, 503, 504}


class AsyncHTTP:
    """
    Minimal async HTTP wrapper with exponential backoff for transient statuses + network errors.
    """

    def __init__(
        self,
        *,
        user_agent: str,
        timeout_s: float,
        max_retries: int,
        backoff_base_s: float,
        backoff_max_s: float,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(
            headers={
                "User-Agent": user_agent,
                "Accept": "application/json, text/plain, */*",
            },
            timeout=httpx.Timeout(timeout_s),
            follow_redirects=True,
        )
        self._max_retries = max_retries
        self._backoff_base_s = backoff_base_s
        self._backoff_max_s = backoff_max_s

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def request_json(
        self,
        url: str,
        *,
        method: HttpMethod = "GET",
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        last_exc: Exception | None = None

        for attempt in range(self._max_retries + 1):
            try:
                resp = await self._client.request(method, url, params=params, data=data)
            except httpx.RequestError as e:
                last_exc = e
                await self._sleep_backoff(attempt)
                continue

            if resp.status_code in TRANSIENT_STATUS:
                last_exc = TransportError(f"Transient HTTP {resp.status_code} from {url}: {resp.text[:400]}")
                await self._sleep_backoff(attempt, retry_after=resp.headers.get("Retry-After"))
                continue

            if resp.status_code < 200 or resp.status_code >= 300:
                raise TransportError(f"HTTP {resp.status_code} from {url}: {resp.text[:800]}")

            try:
                return resp.json()
            except ValueError as e:
                raise TransportError(f"Non-JSON response from {url}: {resp.text[:800]}") from e

        raise TransportError(f"Request failed after retries for {url}: {last_exc}") from last_exc

    async def _sleep_backoff(self, attempt: int, retry_after: str | None = None) -> None:
        if retry_after:
            try:
                ra = float(retry_after)
                await asyncio.sleep(min(ra, self._backoff_max_s))
                return
            except ValueError:
                pass

        # exponential + jitter
        base = self._backoff_base_s * (2 ** attempt)
        jitter = random.uniform(0.0, 0.25)
        await asyncio.sleep(min(base + jitter, self._backoff_max_s))
