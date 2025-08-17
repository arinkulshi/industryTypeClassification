# scoring.py
from __future__ import annotations
from dataclasses import dataclass, asdict
from typing import List, Dict, Optional, Any, Tuple
import re
from difflib import SequenceMatcher
from datetime import datetime

from sec_client import SecClient

try:
    from candidate_generation import Candidate
except Exception:
    @dataclass
    class Candidate:
        cik10: str; edgar_name: str; name_score: float

# ---------- normalization helpers ----------
_WS = re.compile(r"\s+")
_NON_ALNUM = re.compile(r"[^A-Z0-9 ]+")
_DIGITS = re.compile(r"\D+")

CANON = {
    "ST": "SAINT", "ST.": "SAINT", "SAINT": "SAINT",
    "FT": "FORT",  "FT.": "FORT",  "FORT": "FORT",
    "MT": "MOUNT", "MT.": "MOUNT", "MOUNT": "MOUNT",
    "N": "NORTH",  "S": "SOUTH",   "E": "EAST",   "W": "WEST",
}

def _norm_zip5(z: str) -> str:
    if not z: return ""
    digits = _DIGITS.sub("", str(z))
    return digits[:5] if len(digits) >= 5 else digits.zfill(5)

def _norm_city(s: str) -> str:
    if not s: return ""
    s = s.upper()
    s = _NON_ALNUM.sub(" ", s)
    toks = [CANON.get(t, t) for t in _WS.split(s) if t]
    return " ".join(toks)

def token_set_ratio(a: str, b: str) -> float:
    a = _norm_city(a); b = _norm_city(b)
    if not a or not b: return 0.0
    a_set, b_set = set(a.split()), set(b.split())
    inter = " ".join(sorted(a_set & b_set))
    a_rem = " ".join(sorted(a_set - b_set))
    b_rem = " ".join(sorted(b_set - a_set))
    s1 = SequenceMatcher(None, inter, (inter + " " + a_rem).strip()).ratio()
    s2 = SequenceMatcher(None, inter, (inter + " " + b_rem).strip()).ratio()
    return max(s1, s2)

def _parse_date_iso(s: Optional[str]) -> Tuple[int,int,int]:
    try:
        dt = datetime.strptime(s, "%Y-%m-%d"); return (dt.year, dt.month, dt.day)
    except Exception:
        return (0,0,0)

# ---------- output schema ----------
@dataclass
class RankedCandidate:
    cik10: str
    edgar_name: str
    name_score: float
    addr_score: float
    total_score: float
    matched_city: Optional[str]
    matched_zip: Optional[str]
    form: Optional[str]
    accession: Optional[str]
    filingDate: Optional[str]
    header_url: Optional[str]  # always None in offline-only mode

# ---------- submissions helpers ----------
def _addresses_from_submissions(subs: Dict[str, Any]) -> Dict[str, Dict[str, str]]:
    out = {"business": {}, "mail": {}}
    if not subs: return out
    addrs = subs.get("addresses") or {}
    for src_key, dst_key in (("business", "business"), ("mailing", "mail")):
        b = addrs.get(src_key) or {}
        city = (b.get("city") or "").strip().upper()
        state = (b.get("state") or b.get("stateOrCountry") or "").strip().upper()
        zip_raw = b.get("zipCode") or b.get("zip") or ""
        zip5 = _norm_zip5(zip_raw)
        block = {}
        if city:  block["city"] = city
        if state: block["state"] = state[:2]
        if zip5:  block["zip5"] = zip5
        out[dst_key] = block if block else {}
    return out

def _pick_latest_index_from_subs(subs: Dict[str, Any]) -> Optional[int]:
    recent = (subs or {}).get("filings", {}).get("recent", {})
    forms = recent.get("form") or []
    dates = recent.get("filingDate") or []
    if not forms: return None
    idxs = list(range(len(forms)))
    idxs.sort(key=lambda i: (dates[i] or ""), reverse=True)
    for i in idxs:
        if forms[i] in ("10-K", "20-F"):
            return i
    return idxs[0] if idxs else None

def _meta_from_subs(subs: Dict[str, Any]) -> Dict[str, Optional[str]]:
    if not subs: return {"form": None, "accession": None, "filingDate": None}
    recent = subs.get("filings", {}).get("recent", {})
    accs = recent.get("accessionNumber") or []
    forms = recent.get("form") or []
    dates = recent.get("filingDate") or []
    idx = _pick_latest_index_from_subs(subs)
    if idx is None: return {"form": None, "accession": None, "filingDate": None}
    return {
        "form": forms[idx] if idx < len(forms) else None,
        "accession": accs[idx] if idx < len(accs) else None,
        "filingDate": dates[idx] if idx < len(dates) else None,
    }

# ---------- main ----------
def rank_candidates(
    candidates: List[Candidate],
    city: str,
    zip5: str,
    client: Optional[SecClient] = None,
    limit: int = 10,
) -> List[Dict[str, Any]]:
    """
    OFFLINE scoring:
      - uses ONLY Submissions JSON (addresses + recent filing meta)
      - NO network fallback
    """
    if not candidates: return []
    if client is None:
        raise ValueError("Provide `client` (offline SecClient with submissions_store).")

    city_in = _norm_city(city or "")
    zip_in  = _norm_zip5(zip5 or "")

    ranked: List[RankedCandidate] = []
    _cache: Dict[str, Optional[Dict[str, Any]]] = {}

    for cand in candidates:
        cik10 = str(cand.cik10)
        subs = _cache.get(cik10)
        if subs is None:
            subs = client.get_submissions(cik10)
            _cache[cik10] = subs

        addresses = _addresses_from_submissions(subs or {})
        meta = _meta_from_subs(subs or {})

        matched_city = None; matched_zip = None
        city_score = 0.0; zip_score = 0.0

        for key in ("business", "mail"):
            block = addresses.get(key) or {}
            c = _norm_city(block.get("city") or "")
            z = _norm_zip5(block.get("zip5") or block.get("zip") or "")
            s = token_set_ratio(city_in, c) if c else 0.0
            if s > city_score:
                city_score = s; matched_city = (block.get("city") or matched_city)
            if z and zip_in and z == zip_in:
                zip_score = 1.0; matched_zip = z

        addr_score = city_score + zip_score
        total_score = addr_score + 0.5 * float(cand.name_score)

        ranked.append(RankedCandidate(
            cik10=cik10, edgar_name=cand.edgar_name, name_score=float(cand.name_score),
            addr_score=float(addr_score), total_score=float(total_score),
            matched_city=matched_city, matched_zip=matched_zip,
            form=meta.get("form"), accession=meta.get("accession"), filingDate=meta.get("filingDate"),
            header_url=None,  # offline-only
        ))

    ranked.sort(key=lambda rc: (-rc.total_score, -rc.addr_score, _parse_date_iso(rc.filingDate)), reverse=False)
    return [asdict(rc) for rc in ranked[:limit]]
