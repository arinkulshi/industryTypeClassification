# submissions_store.py
from __future__ import annotations
import json
from pathlib import Path
from typing import Optional
from zipfile import ZipFile, BadZipFile, ZIP_DEFLATED

class SubmissionsStore:
    """
    Lightweight reader for SEC bulk submissions.zip.
    Expects paths like: submissions/CIK##########.json
    Returns the parsed JSON dict, or None if not found.
    """
    def __init__(self, zip_path: str):
        self.path = Path(zip_path)
        if not self.path.exists():
            raise FileNotFoundError(f"submissions.zip not found at {self.path}")
        # Lazy-open on first access to avoid holding handle if not needed
        self._zip: Optional[ZipFile] = None
        self._cache: dict[str, Optional[dict]] = {}  # memoize parsed JSON by CIK10

    def _ensure_open(self):
        if self._zip is None:
            try:
                self._zip = ZipFile(self.path, mode="r")
            except BadZipFile as e:
                raise RuntimeError(f"Invalid submissions.zip at {self.path}") from e

    @staticmethod
    def _cik10(cik: str | int) -> str:
        return f"{int(str(cik).strip()):010d}"

    def get(self, cik: str | int) -> Optional[dict]:
        """
        Return parsed submissions JSON for a CIK (10-digit, zero-padded).
        If the file doesn't exist in the ZIP, returns None.
        """
        cik10 = self._cik10(cik)
        if cik10 in self._cache:
            return self._cache[cik10]

        self._ensure_open()

        # Primary expected location
        possible = [
            f"submissions/CIK{cik10}.json",
            f"CIK{cik10}.json",  # fallback, in case some releases change paths
        ]
        data = None
        for member in possible:
            try:
                with self._zip.open(member) as fp:
                    data = json.loads(fp.read().decode("utf-8"))
                    break
            except KeyError:
                continue

        self._cache[cik10] = data
        return data
