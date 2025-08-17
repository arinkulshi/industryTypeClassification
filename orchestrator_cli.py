# orchestrator_cli.py
import argparse
from orchestrator import Orchestrator
from submissions_store import SubmissionsStore

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--names-json", required=True, help="Path to name_to_ciks.json")
    p.add_argument("--in", dest="inp", required=True, help="Input CSV")
    p.add_argument("--out", required=True, help="Output CSV")
    p.add_argument("--name-col", help="Column for company name (normalized or raw)")
    p.add_argument("--city-col", help="Column for city")
    p.add_argument("--zip-col", help="Column for zip/postal code")
    p.add_argument("--threshold", type=float, default=0.85)
    p.add_argument("--limit", type=int, default=10)
    p.add_argument("--min-accept", type=float, default=1.6)
    p.add_argument("--gap-accept", type=float, default=0.3)
    p.add_argument("--cache-dir", default="./sec_cache")
    p.add_argument("--audit", dest="audit_jsonl", default=None)
    p.add_argument("--submissions-path", required=True,
                   help="Path to SEC bulk submissions.zip OR directory containing CIK##########.json files")
    p.add_argument("--no-force-ambiguous", action="store_true",
                   help="If set, do NOT return industry_subtype for ambiguous rows")
    args = p.parse_args()

    store = SubmissionsStore(args.submissions_path)

    orc = Orchestrator(
        names_json=args.names_json,
        cache_dir=args.cache_dir,
        force_ambiguous=not args.no_force_ambiguous,
        submissions_store=store,
    )

    rows = orc.run_csv(
        csv_in=args.inp,
        csv_out=args.out,
        name_col=args.name_col,
        city_col=args.city_col,
        zip_col=args.zip_col,
        threshold=args.threshold,
        limit=args.limit,
        min_accept=args.min_accept,
        gap_accept=args.gap_accept,
        write_audit_jsonl=args.audit_jsonl,
    )
    print(f"Wrote {args.out} with {len(rows)} rows")

if __name__ == "__main__":
    main()
