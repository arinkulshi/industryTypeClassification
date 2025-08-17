# sec_client.py
from __future__ import annotations
from typing import Any, Dict, Optional

def _normalize_cik10(cik: str | int) -> str:
    return f"{int(str(cik).strip()):010d}"

class SecClient:
    """
    OFFLINE-ONLY SEC client.
    All data must come from a SubmissionsStore (zip or directory).
    No HTTP, no User-Agent, no rate limiting.
    """
    def __init__(self, submissions_store):
        if submissions_store is None:
            raise ValueError("SecClient requires a submissions_store (zip or directory).")
        # Keep type loose: we only call `.get(cik10)`
        self.submissions_store = submissions_store

    def get_submissions(self, cik: str | int) -> Optional[Dict[str, Any]]:
        cik10 = _normalize_cik10(cik)
        try:
            return self.submissions_store.get(cik10)
        except Exception:
            return None
