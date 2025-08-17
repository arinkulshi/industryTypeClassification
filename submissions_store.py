# submissions_store.py
from __future__ import annotations
import json
from pathlib import Path
from typing import Optional, Union
from zipfile import ZipFile, BadZipFile

class SubmissionsStore:
    """
    Reads Submissions JSON from either:
      - a bulk ZIP (submissions.zip), or
      - an extracted directory tree containing CIK##########.json files.

    Expected layouts (any of these work):
      <root>/submissions.zip
      <root>/submissions/CIK##########.json
      <root>/CIK##########.json
    """
    def __init__(self, path: Union[str, Path]):
        self.root = Path(path)
        if not self.root.exists():
            raise FileNotFoundError(f"Submissions path not found: {self.root}")

        self._mode: str
        self._zip: Optional[ZipFile] = None
        self._base_dir: Optional[Path] = None
        self._cache: dict[str, Optional[dict]] = {}

        if self.root.is_file():
            # ZIP mode
            self._mode = "zip"
            try:
                self._zip = ZipFile(self.root, mode="r")
            except BadZipFile as e:
                raise RuntimeError(f"Invalid submissions.zip at {self.root}") from e
        else:
            # DIR mode
            self._mode = "dir"
            # Heuristic: prefer "<root>/submissions" if present, else "<root>"
            subdir = self.root / "submissions"
            self._base_dir = subdir if subdir.exists() and subdir.is_dir() else self.root

    @staticmethod
    def _cik10(cik: str | int) -> str:
        return f"{int(str(cik).strip()):010d}"

    def get(self, cik: str | int) -> Optional[dict]:
        cik10 = self._cik10(cik)
        if cik10 in self._cache:
            return self._cache[cik10]

        data: Optional[dict] = None
        if self._mode == "zip":
            # Try common locations inside the ZIP
            for member in (f"submissions/CIK{cik10}.json", f"CIK{cik10}.json"):
                try:
                    with self._zip.open(member) as fp:  # type: ignore[union-attr]
                        data = json.loads(fp.read().decode("utf-8"))
                        break
                except KeyError:
                    continue
        else:
            # Directory mode: try a few likely paths without walking the tree each time
            candidates = [
                self._base_dir / f"CIK{cik10}.json",            # <root>/CIK*.json  OR  <root>/submissions/CIK*.json
                self.root / f"CIK{cik10}.json",                 # <root>/CIK*.json
                (self.root / "submissions" / f"CIK{cik10}.json")# <root>/submissions/CIK*.json (redundant but safe)
            ]
            for p in candidates:
                if p.exists():
                    data = json.loads(p.read_text(encoding="utf-8"))
                    break

        self._cache[cik10] = data
        return data

    def close(self):
        if self._zip:
            self._zip.close()
            self._zip = None
