# score_cli.py
import argparse, json
from pathlib import Path
import pandas as pd

from submissions_store import SubmissionsStore
from sec_client import SecClient
from candidate_generation import generate_candidates
from scoring import rank_candidates

def load_name_map(path: str) -> dict:
    j = json.loads(Path(path).read_text())
    return j.get("map", j)

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--names-json", required=True)
    p.add_argument("--submissions-path", required=True)
    p.add_argument("--name")
    p.add_argument("--city")
    p.add_argument("--zip5")
    p.add_argument("--csv")
    p.add_argument("--name-col")
    p.add_argument("--city-col")
    p.add_argument("--zip-col")
    p.add_argument("--out", default="ranked.jsonl")
    p.add_argument("--threshold", type=float, default=0.85)
    p.add_argument("--limit", type=int, default=10)
    args = p.parse_args()

    name_map = load_name_map(args.names_json)
    client = SecClient(submissions_store=SubmissionsStore(args.submissions_path))

    if args.name:
        if args.city is None or args.zip5 is None:
            p.error("Provide --city and --zip5.")
        cands = generate_candidates(args.name, name_map, threshold=args.threshold, limit=args.limit)
        ranked = rank_candidates(cands, args.city, args.zip5, client=client, limit=args.limit)
        print(json.dumps({"query": args.name, "results": ranked}, indent=2)); return

    if args.csv:
        df = pd.read_csv(args.csv, dtype=str)
        name_col = args.name_col or "normalized_name"
        city_col = args.city_col or "city"
        zip_col = args.zip_col or "zip"
        with open(args.out, "w") as f:
            for _, row in df.iterrows():
                name = str(row[name_col]); city = str(row[city_col]); zip5 = str(row[zip_col])[:5]
                cands = generate_candidates(name, name_map, threshold=args.threshold, limit=args.limit)
                ranked = rank_candidates(cands, city, zip5, client=client, limit=args.limit)
                f.write(json.dumps({"query": {"name": name, "city": city, "zip5": zip5}, "results": ranked}) + "\n")
        print(f"Wrote {args.out}"); return

    p.print_help()

if __name__ == "__main__":
    main()
