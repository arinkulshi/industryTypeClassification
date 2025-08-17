Chunk 1 — Name Normalization

Goal: Canonically normalize company names to improve matching.
Input: company_name: str
Output: normalized_name: str
Rules (deterministic):

Uppercase.

Remove punctuation.

Strip corporate suffixes: INC|INCORPORATED|LLC|L.L.C.|LTD|LIMITED|PLC|CO|COMPANY|CORP|CORPORATION|HOLDINGS?.

Collapse whitespace to single spaces; trim ends.
Acceptance checks:

“The ACME Co., Inc.” → “ACME”

“Globex LLC” → “GLOBEX”

Idempotent on already-clean inputs.

Chunk 2 — Local SEC Index Loader

Goal: Build/read local lookup tables (so we’re not hammering SEC per request).
Input: None (config path).
Output: Two structures:

ticker_map: Dict[TICKER -> CIK10]

name_to_ciks: Dict[NORM_NAME -> Set[CIK10]]
Rules:

Download once and cache:

company_tickers.json → tickers + CIKs + titles (normalize titles to keys).

“CIK <-> entity name” bulk text (broad coverage). Normalize names.

Persist as local JSON files; include fetched_at.

Do not dedupe CIK collisions—store all.
Acceptance checks:

Re-running uses cached files if fresh (<24h).

ticker_map["AAPL"] exists and is zero-padded CIK.

name_to_ciks["APPLE"] contains Apple’s CIK.

Chunk 3 — Candidate Generation (Name → CIK candidates)

Goal: From (normalized_name) generate CIK candidates.
Input: normalized_name: str, name_to_ciks
Output: List[Candidate] where Candidate = {cik10, edgar_name, name_score}
Rules:

Exact key match first.

If not found, compute token-set similarity against all keys (threshold ≥ 0.85). (Implement efficient index—e.g., first-letter bucket and length band.)

name_score is the similarity [0..1].

Limit to top 10.
Acceptance checks:

Exact match outranks fuzzy.

Returns empty list if no candidates ≥ 0.85.

Chunk 4 — SEC Submissions Fetcher (Polite Client)

Goal: Fetch submissions/CIK{cik10}.json safely.
Input: cik10: str
Output: SubmissionsJSON (raw dict)
Rules:

Add real User-Agent header; gzip enabled.

Retries with exponential backoff and jitter; max 3 attempts.

Rate limit ≤ 10 req/sec (token bucket).

On HTTP 404 → return None (record error).
Acceptance checks:

5xx triggers retry then surfaces structured error.

Successful parse returns keys filings.recent.form, etc.

Chunk 5 — Latest Filing Header Reader

Goal: For a candidate CIK, fetch the latest 10-K/20-F header (fallback: any recent filing) and extract BUSINESS/MAIL address.
Input: SubmissionsJSON
Output:
