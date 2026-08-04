"""
Shared eDesk API client: authentication, rate limiting, and retry/backoff
logic used by every eDesk pipeline (edesk_tickets.py, edesk_messages.py,
and any future resource-specific pipeline).

This module is intentionally resource-agnostic -- it knows nothing about
tickets, messages, sales orders, etc. Resource-specific pipelines
subclass `EDeskClient` and add their own methods (see EDeskTicketsClient
in edesk_tickets.py, EDeskMessagesClient in edesk_messages.py).

Docs used:
- Auth:          https://developers.edesk.com/reference/authentication
    Bearer token (generate at https://dashboard.edesk.com/api-token)
- Rate limit:    https://developers.edesk.com/reference/rate-limit.md
    Burst capacity: 60 requests. Refill rate: 2 requests/second.
    Exceeding the limit returns HTTP 429; back off and retry.
- Error codes:   https://developers.edesk.com/reference/error-codes.md
    (4xxx codes apply to write endpoints; this base client only handles
    the generic {"error": {"httpCode", "message", "details"}} shape.)
"""

from __future__ import annotations
import time
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, Optional
import requests
import logger_setup


logger_setup.setup_logging(__file__)
logger = logging.getLogger(__name__)

BASE_URL = "https://api.edesk.com/v1"


class EDeskAPIError(Exception):
    """Raised for non-retryable eDesk API errors."""


def _describe_error_response(resp: requests.Response) -> str:
    """
    Build a human-readable message from an eDesk error response.

    Expected shape (from https://developers.edesk.com/reference/rate-limit.md
    and the OpenAPI BaseErrorResponse schema):
        {"error": {"httpCode": 429, "message": "...", "details": "..."}}

    Falls back to the raw response text if the body isn't the expected shape.
    """
    try:
        body = resp.json()
    except ValueError:
        return resp.text

    error = body.get("error") if isinstance(body, dict) else None
    if not isinstance(error, dict):
        return resp.text

    parts = [str(error[k]) for k in ("message", "details") if error.get(k)]
    return " -- ".join(parts) if parts else resp.text


@dataclass
class TokenBucketRateLimiter:
    """
    Token bucket matching eDesk's documented rate limit:
    https://developers.edesk.com/reference/rate-limit.md

    - Burst capacity: 60 requests
    - Refill rate: 2 requests / second
    """
    capacity: float = 60.0
    refill_rate: float = 2.0  # tokens per second
    _tokens: float = field(init=False, default=0.0)
    _last_refill: float = field(init=False, default=0.0)

    def __post_init__(self) -> None:
        self._tokens = self.capacity
        self._last_refill = time.monotonic()

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(self.capacity, self._tokens + elapsed * self.refill_rate)
        self._last_refill = now

    def acquire(self) -> None:
        """Block until a token is available, then consume one."""
        while True:
            self._refill()
            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return
            deficit = 1.0 - self._tokens
            wait_s = deficit / self.refill_rate
            time.sleep(max(wait_s, 0.01))


class EDeskClient:
    """
    Generic authenticated eDesk API client: handles bearer auth, rate
    limiting, and retry/backoff on 429s and 5xxs. Has no knowledge of any
    specific resource (tickets, messages, ...) -- subclass this and add
    resource-specific methods that call `self._request(...)`.
    """

    def __init__(
        self,
        api_token: str,
        base_url: str = BASE_URL,
        max_retries: int = 5,
        session: Optional[requests.Session] = None,
    ):
        self.api_token = api_token
        self.base_url = base_url.rstrip("/")
        self.max_retries = max_retries
        self.session = session or requests.Session()
        self.rate_limiter = TokenBucketRateLimiter()

    def _request(self, method: str, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        url = f"{self.base_url}{path}"
        headers = {
            "Authorization": f"Bearer {self.api_token}",
            "Accept": "application/json",
        }
        # Drop empty/None params so we don't send unwanted filters.
        clean_params = {k: v for k, v in (params or {}).items() if v is not None}

        for attempt in range(1, self.max_retries + 1):
            self.rate_limiter.acquire()
            resp = self.session.request(method, url, headers=headers, params=clean_params, timeout=30)

            if resp.status_code == 401:
                # Not retryable -- retrying won't fix an invalid/expired
                # token. eDesk API tokens default to a 90-day expiry with
                # no programmatic refresh (see
                # https://support.edesk.com/how-to-generate-a-token-edesk-api),
                # so this is almost always "go generate a new one."
                raise EDeskAPIError(
                    f"401 Unauthorized on {method} {url} -- eDesk API token is "
                    f"invalid or expired (tokens default to a 90-day expiry, "
                    f"renewed manually at https://dashboard.edesk.com/api-token). "
                    f"{_describe_error_response(resp)}"
                )

            if resp.status_code == 429:
                # Rate limited -- respect Retry-After if present, else back off.
                retry_after = resp.headers.get("Retry-After")
                wait_s = float(retry_after) if retry_after else min(2 ** attempt, 30)
                logger.warning("429 rate limited on %s, waiting %.1fs (attempt %d/%d): %s",
                                path, wait_s, attempt, self.max_retries, _describe_error_response(resp))
                time.sleep(wait_s)
                continue

            if resp.status_code >= 500:
                wait_s = min(2 ** attempt, 30)
                logger.warning("Server error %s on %s, retrying in %.1fs (attempt %d/%d): %s",
                                resp.status_code, path, wait_s, attempt, self.max_retries,
                                _describe_error_response(resp))
                time.sleep(wait_s)
                continue

            if not resp.ok:
                raise EDeskAPIError(
                    f"{method} {url} failed: {resp.status_code} {_describe_error_response(resp)}"
                )

            if resp.content:
                return resp.json()
            return None

        raise EDeskAPIError(f"{method} {url} failed after {self.max_retries} retries")
