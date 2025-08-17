# orchestrator.py
from __future__ import annotations
import json
import re
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

from sec_client import SecClient
from candidate_generation import generate_candidates
from scoring import rank_candidates
from resolution import resolve_cik
from sic_retrieval import SicResolver

# Try to reuse the normalizer from earlier chunks; fallback to a local one.
try:
    from sec_normalize import normalize_company_name as _norm_name
except Exception:
    import re
    _SUFFIX = re.compile(r"(?:\s+|^)(?:CORPORATION|CORP|INCORPORATED|INC|LLC|L\.L\.C\.|LTD|LIMITED|PLC|CO|COMPANY|HOLDING|HOLDINGS)(?:\s+|$)", re.I)
    _PUNCT  = re.compile(r"[^A-Z0-9]+")
    def _norm_name(s: str) -> str:
        s = (s or "").upper().strip()
        s = _PUNCT.sub(" ", s)
        s = re.sub(r"\s+", " ", s).strip()
        prev = None
        while prev != s and s:
            prev = s
            s = " " + s + " "
            s = _SUFFIX.sub(" ", s)
            s = re.sub(r"\s+", " ", s).strip()
        return s

def _find_col(cols, patterns):
    lower = {c.lower(): c for c in cols}
    for pat in patterns:
        rx = re.compile(pat, re.I)
        for lc, orig in lower.items():
            if rx.fullmatch(lc) or rx.search(lc):
                return orig
    return None

def _auto_columns(df, name_col, city_col, zip_col):
    cols = list(df.columns)
    if not name_col:
        name_col = _find_col(cols, [r"^normalized_name$", r"^company(\s*name)?$", r"^name$"])
    if not city_col:
        city_col = _find_col(cols, [r"^city$"])
    if not zip_col:
        zip_col = _find_col(cols, [r"^zip$", r"^zip[_\s-]?code$", r"^postal[_\s-]?code$", r"^postal$"])
    return name_col, city_col, zip_col

def _load_name_map(path: str) -> dict:
    j = json.loads(Path(path).read_text())
    return j.get("map", j)

class Orchestrator:
    """
    End-to-end pipeline:
      Input: (name, city, zip) per row
      Output: one industry sub-type (sicDescription)
    """
    def __init__(
        self,
        user_agent: str,
        names_json: str,
        cache_dir: str = "./sec_cache",
        force_ambiguous: bool = True,
        submissions_store: Optional[object] = None,   # NEW: pass-through to SecClient
    ):
        self.client = SecClient(user_agent=user_agent, submissions_store=submissions_store)
        self.name_map = _load_name_map(names_json)
        self.sic_resolver = SicResolver(self.client, cache_dir=cache_dir, ttl_hours=24)
        self.force_ambiguous = bool(force_ambiguous)

    def run_row(
        self,
        name: str,
        city: str,
        zip_code: str,
        *,
        threshold: float = 0.85,
        limit: int = 10,
        min_accept: float = 1.6,
        gap_accept: float = 0.3,
    ) -> Dict[str, Optional[str]]:
        """
        Returns:
          {
            status: "ok"|"ambiguous"|"not_found",
            cik10: str|None,
            industry_subtype: str|None,   # sicDescription
            sic: str|None,
            sic_description: str|None,
            reason: str
          }
        """
        name_norm = _norm_name(name)
        city = (city or "").strip()
        zip5 = (zip_code or "").strip()[:5]

        # Chunks 3-6
        cands = generate_candidates(name_norm, self.name_map, threshold=threshold, limit=limit)
        ranked = rank_candidates(cands, city, zip5, client=self.client, limit=limit)

        # Chunk 7
        final = resolve_cik(ranked, zip5, min_accept=min_accept, gap_accept=gap_accept, keep_top=3)
        status = final.get("status", "not_found")
        out = {
            "status": status,
            "cik10": final.get("cik10"),
            "reason": final.get("reason"),
            "industry_subtype": None,
            "sic": None,
            "sic_description": None,
        }

        if status == "ok" and final.get("cik10"):
            info = self.sic_resolver.get_sic(final["cik10"])
            if info:
                out["sic"] = info.get("sic")
                out["sic_description"] = info.get("sicDescription")
                out["industry_subtype"] = info.get("sicDescription")
        return out

    def run_csv(
        self,
        csv_in: str,
        csv_out: str,
        *,
        name_col: Optional[str] = None,
        city_col: Optional[str] = None,
        zip_col: Optional[str] = None,
        threshold: float = 0.85,
        limit: int = 10,
        min_accept: float = 1.6,
        gap_accept: float = 0.3,
        write_audit_jsonl: Optional[str] = None,
    ) -> List[Dict]:
        """
        Read input CSV, write output CSV with 'industry_subtype' and related fields.
        Returns the list of result dicts (one per processed row).
        """
        df = pd.read_csv(csv_in, dtype=str)
        ncol, ccol, zcol = _auto_columns(df, name_col, city_col, zip_col)
        if not all([ncol, ccol, zcol]):
            missing = []
            if not ncol: missing.append("name")
            if not ccol: missing.append("city")
            if not zcol: missing.append("zip")
            raise SystemExit(f"CSV missing required column(s): {', '.join(missing)}. "
                             f"Use name_col/city_col/zip_col params or rename columns.")

        results: List[Dict] = []
        audit_f = open(write_audit_jsonl, "w") if write_audit_jsonl else None

        for _, r in df.iterrows():
            raw_name = (r[ncol] or "").strip()
            city = (r[ccol] or "").strip()
            zip_code = (r[zcol] or "").strip()

            if not raw_name or not city or not zip_code:
                results.append({
                    "name": raw_name, "city": city, "zip5": zip_code[:5],
                    "status": "not_found", "reason": "missing inputs",
                    "cik10": "", "sic": "", "sic_description": "", "industry_subtype": ""
                })
                continue

            # Generate + score for audit (without SIC yet)
            name_norm = _norm_name(raw_name)
            cands = generate_candidates(name_norm, self.name_map, threshold=threshold, limit=limit)
            ranked = rank_candidates(cands, city, zip_code[:5], client=self.client, limit=limit)
            final = resolve_cik(ranked, zip_code[:5], min_accept=min_accept, gap_accept=gap_accept, keep_top=3)

            if audit_f:
                audit_f.write(json.dumps({
                    "query": {"name": name_norm, "city": city, "zip5": zip_code[:5]},
                    "ranked": ranked,
                    "final": final
                }) + "\n")

            row_out = {
                "name": raw_name,
                "city": city,
                "zip5": zip_code[:5],
                "status": final.get("status", "not_found"),
                "reason": final.get("reason", ""),
                "cik10": final.get("cik10", ""),
                "sic": "",
                "sic_description": "",
                "industry_subtype": "",
            }

            if row_out["status"] == "ok" and row_out["cik10"]:
                info = self.sic_resolver.get_sic(row_out["cik10"])
                if info:
                    row_out["sic"] = info.get("sic") or ""
                    row_out["sic_description"] = info.get("sicDescription") or ""
                    row_out["industry_subtype"] = info.get("sicDescription") or ""

            results.append(row_out)

        if audit_f:
            audit_f.close()

        out_df = pd.DataFrame(results)
        out_df.to_csv(csv_out, index=False)
        return results
