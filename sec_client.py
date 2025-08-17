# sec_client.py (only changed bits shown; paste this whole file if easier)
from __future__ import annotations
import json
import random
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple, ClassVar

import requests

def _now() -> float:
    return time.monotonic()

def _normalize_cik10(cik: str | int) -> str:
    try:
        return f"{int(str(cik).strip()):010d}"
    except Exception as e:
        raise ValueError(f"Invalid CIK: {cik}") from e

class TokenBucket:
    # ... (unchanged)
    _lock: threading.Lock
    _last_refill: float
    _tokens: float
    def __init__(self, rate: float, per: float):
        self.rate = float(rate)
        self.per = float(per)
        self.capacity = float(rate)
        self._lock = threading.Lock()
        self._last_refill = _now()
        self._tokens = self.capacity
    def acquire(self, tokens: float = 1.0):
        while True:
            with self._lock:
                now = _now()
                elapsed = now - self._last_refill
                if elapsed > 0:
                    self._tokens = min(self.capacity, self._tokens + (elapsed * self.rate / self.per))
                    self._last_refill = now
                if self._tokens >= tokens:
                    self._tokens -= tokens
                    return
                needed = tokens - self._tokens
                wait = (needed * self.per) / self.rate
            time.sleep(wait)

@dataclass
class SecHttpError(Exception):
    status: int
    url: str
    body_snippet: str
    def __str__(self):
        return f"SEC HTTP error {self.status} for {self.url}: {self.body_snippet[:200]}"

class SecClient:
    """
    Polite SEC client for Submissions JSON + optional bulk store.
    If `submissions_store` is provided, `get_submissions` returns from the ZIP
    and will NOT call the network (source of truth = local).
    """
    _bucket: ClassVar[TokenBucket] = TokenBucket(rate=10.0, per=1.0)

    def __init__(
        self,
        user_agent: str,
        timeout: int = 60,
        max_tries: int = 3,
        session: Optional[requests.Session] = None,
        submissions_store: Optional[object] = None,  # SubmissionsStore, but typed loosely to avoid hard dep
    ):
        if not user_agent or "@" not in user_agent:
            raise ValueError("User-Agent must include contact info (e.g., 'YourOrg you@example.com').")
        self.timeout = timeout
        self.max_tries = max(1, int(max_tries))
        self.session = session or requests.Session()
        self.session.headers.update(
            {"User-Agent": user_agent, "Accept-Encoding": "gzip, deflate"}
        )
        self.submissions_store = submissions_store  # if set, used as source of truth

    # --- new helper to optionally serve from ZIP
    def get_submissions(self, cik: str | int) -> Optional[Dict[str, Any]]:
        cik10 = _normalize_cik10(cik)

        # Prefer local store if present (no network)
        if self.submissions_store is not None:
            try:
                doc = self.submissions_store.get(cik10)  # type: ignore[attr-defined]
            except Exception:
                doc = None
            return doc  # could be None if not in ZIP

        # Otherwise, hit the live endpoint (old behavior)
        url = f"https://data.sec.gov/submissions/CIK{cik10}.json"
        return self._get_json(url)

    # --- unchanged below here ---
    def _get_json(self, url: str) -> Optional[Dict[str, Any]]:
        for attempt in range(1, self.max_tries + 1):
            self._bucket.acquire(1.0)
            try:
                resp = self.session.get(url, timeout=self.timeout)
            except requests.RequestException as e:
                if attempt >= self.max_tries:
                    raise SecHttpError(-1, url, f"Network error: {e}") from e
                self._sleep_backoff(attempt)
                continue
            if resp.status_code == 404:
                return None
            if 200 <= resp.status_code < 300:
                try:
                    return resp.json()
                except ValueError as e:
                    if attempt >= self.max_tries:
                        raise SecHttpError(resp.status_code, url, f"Invalid JSON: {resp.text[:200]}") from e
                    self._sleep_backoff(attempt); continue
            if resp.status_code in (429, 500, 502, 503, 504):
                if attempt >= self.max_tries:
                    raise SecHttpError(resp.status_code, url, resp.text[:500])
                self._sleep_backoff(attempt, server_retry_after=resp.headers.get("Retry-After")); continue
            raise SecHttpError(resp.status_code, url, resp.text[:500])
        raise SecHttpError(-1, url, "Exhausted retries")

    def _sleep_backoff(self, attempt: int, server_retry_after: Optional[str] = None):
        if server_retry_after:
            try:
                secs = float(server_retry_after)
                time.sleep(min(max(secs, 0.5), 10.0)); return
            except Exception:
                pass
        base = 0.5 * (2 ** (attempt - 1))
        jitter = random.uniform(0.0, 0.3)
        time.sleep(min(base + jitter, 5.0))
