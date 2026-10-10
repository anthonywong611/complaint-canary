"""Shared HTTP client settings."""

from __future__ import annotations

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

TRANSIENT = (httpx.TransportError, httpx.HTTPStatusError)

# Three retries with exponential backoff, then give up and let the run fail.
retry_transient = retry(
    retry=retry_if_exception_type(TRANSIENT),
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=2, min=2, max=60),
    reraise=True,
)


def make_client(user_agent: str = "") -> httpx.Client:
    # CFPB's CDN rejects some custom User-Agent strings, so the library default is kept
    # unless one is configured.
    headers = {"User-Agent": user_agent} if user_agent else {}
    return httpx.Client(headers=headers, timeout=httpx.Timeout(60.0, read=300.0), follow_redirects=True)


def is_remote(url: str) -> bool:
    return url.startswith(("http://", "https://"))
