# resolve_cli.py
import argparse, json
from pathlib import Path
import pandas as pd

from submissions_store import SubmissionsStore
from sec_client import SecClient
from candidate_generation import generate_candidates
from scoring import rank_candidates
from resolution import resolve_cik

def load_name_map(path: str) -> dict:
    import json
    from pathlib import Path
    j = json.loads(Path(path).read_text()); return j.get("map", j)

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--names-json", required=True)
    p.add_argument("--submissions-path", required=True)
    p.add_argument("--name"); p.add_argument("--city"); p.add_argument("--zip5")
    p.add_argument("--csv"); p.add_argument("--name-col"); p.add_argument("--city-col"); p.add_argument("--zip-col")
    p.add_argument("--out", default="resolved.csv")
    p.add_argument("--threshold", type=float, default=0.85)
    p.add_argument("--limit", type=int, default=10)
    p.add_argument("--min-accept", type=float, default=1.6)
    p.add_argument("--gap-accept", type=float, default=0.3)
    p.add_argument("--audit", default=None)
    args = p.parse_args()

    name_map = load_name_map(args.names_json)
    client = SecClient(submissions_store=SubmissionsStore(args.submissions_path))

    if args.name:
        if not (args.city and args.zip5): p.error("Provide --city and --zip5.")
        cands = generate_candidates(args.name, name_map, threshold=args.threshold, limit=args.limit)
        ranked = rank_candidates(cands, args.city, args.zip5, client=client, limit=args.limit)
        final = resolve_cik(ranked, args.zip5, min_accept=args.min_accept, gap_accept=args.gap_accept, keep_top=3)
        print(json.dumps({"query": {"name": args.name, "city": args.city, "zip5": args.zip5}, **final}, indent=2)); return

    if args.csv:
        df = pd.read_csv(args.csv, dtype=str)
        name_col = args.name_col or "normalized_name"
        city_col = args.city_col or "city"
        zip_col = args.zip_col or "zip"
        rows = []
        audit_f = open(args.audit, "w") if args.audit else None
        for _, r in df.iterrows():
            name = (r[name_col] or "").strip()
            city = (r[city_col] or "").strip()
            zip5 = (r[zip_col] or "").strip()[:5]
            cands = generate_candidates(name, name_map, threshold=args.threshold, limit=args.limit)
            ranked = rank_candidates(cands, city, zip5, client=client, limit=args.limit)
            final = resolve_cik(ranked, zip5, min_accept=args.min_accept, gap_accept=args.gap_accept, keep_top=3)
            if audit_f:
                audit_f.write(json.dumps({"query": {"name": name, "city": city, "zip5": zip5}, "ranked": ranked, "final": final}) + "\n")
            rows.append({"name": name, "city": city, "zip5": zip5, "status": final.get("status","not_found"),
                        "cik10": final.get("cik10",""), "reason": final.get("reason","")})
        if audit_f: audit_f.close()
        import pandas as pd; pd.DataFrame(rows).to_csv(args.out, index=False)
        print(f"Wrote {args.out}"); return

    p.print_help()

if __name__ == "__main__":
    main()
