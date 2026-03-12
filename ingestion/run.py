"""CLI entry point for the Strava ingestion pipeline.

Usage:
    python -m ingestion.run                         # DuckDB, load activities + routes + bikes
    python -m ingestion.run --streams               # also parse FIT/GPX files (slow)
    python -m ingestion.run --streams-only          # streams only (activities must exist)
    python -m ingestion.run --backend postgres      # target PostgreSQL instead of DuckDB
    python -m ingestion.run --backend postgres --dsn postgresql://user:pass@host/strava
    python -m ingestion.run --replace               # truncate tables before loading
    python -m ingestion.run --workers 8             # parallelism for FIT/GPX parsing
    python -m ingestion.run --limit 50              # parse only first N stream files
    python -m ingestion.run --db /path/to/other.duckdb
"""

import argparse
import sys
import time

from ingestion.config import ACTIVITIES_CSV, ACTIVITIES_DIR, DB_PATH, PG_DSN
from ingestion.db import get_backend
from ingestion.loaders import activities as act_loader
from ingestion.loaders import routes as route_loader
from ingestion.loaders import streams as stream_loader


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ingest Strava export into DuckDB or PostgreSQL",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--backend",
        choices=["duckdb", "postgres"],
        default="duckdb",
        help="Database backend (default: duckdb)",
    )
    parser.add_argument(
        "--db",
        default=str(DB_PATH),
        help=f"DuckDB file path (default: {DB_PATH})",
    )
    parser.add_argument(
        "--dsn",
        default=PG_DSN,
        help="PostgreSQL DSN (default: from PG_* env vars / .env)",
    )
    parser.add_argument(
        "--streams",
        action="store_true",
        help="Also parse FIT/GPX activity files into activity_streams (slow)",
    )
    parser.add_argument(
        "--streams-only",
        action="store_true",
        help="Only load streams; skip activities.csv and routes",
    )
    parser.add_argument(
        "--replace",
        action="store_true",
        help="Truncate affected tables before inserting (full reload)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Thread pool size for stream parsing (default: 4)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Parse only the first N activity files (useful for testing)",
    )
    args = parser.parse_args()

    # ── Connect ───────────────────────────────────────────────────────────────
    kwargs = {"db_path": args.db} if args.backend == "duckdb" else {"dsn": args.dsn}
    t0 = time.perf_counter()
    print(f"\nBackend : {args.backend}")
    print(f"Target  : {args.db if args.backend == 'duckdb' else args.dsn}\n")

    with get_backend(args.backend, **kwargs) as db:
        db.create_schema()

        # ── Activities / routes / bikes ───────────────────────────────────────
        if not args.streams_only:
            print(f"Loading {ACTIVITIES_CSV.name} …")
            df = act_loader.load_activities_df()
            n = db.upsert_activities(df)
            db.log_ingestion("activities", n, str(ACTIVITIES_CSV), "activities.csv load")
            print(f"  ✓ {n:,} activities")

            print("Loading saved routes …")
            routes_df, points_by_route = route_loader.load_routes()
            all_points = []
            for pts_df in points_by_route.values():
                all_points.append(pts_df)

            import pandas as pd
            all_points_df = pd.concat(all_points, ignore_index=True) if all_points else pd.DataFrame()
            r, p = db.upsert_routes(routes_df, all_points_df)
            db.log_ingestion("saved_routes", r, str(ACTIVITIES_CSV.parent / "routes.csv"), "routes load")
            db.log_ingestion("saved_route_points", p, str(ACTIVITIES_CSV.parent / "routes"), "GPX load")
            print(f"  ✓ {r} routes, {p:,} GPS points")

            print(f"Loading {ACTIVITIES_CSV.parent / 'bikes.csv'} …")
            bikes_df = route_loader.load_bikes()
            b = db.upsert_bikes(bikes_df)
            db.log_ingestion("bikes", b, str(ACTIVITIES_CSV.parent / "bikes.csv"), "bikes load")
            print(f"  ✓ {b} bikes")

        # ── Streams ───────────────────────────────────────────────────────────
        if args.streams or args.streams_only:
            all_tasks = db.get_activity_files()
            if args.replace:
                already_done: set[int] = set()
            else:
                already_done = db.already_loaded_stream_ids()

            pending = [(aid, fn) for aid, fn in all_tasks if aid not in already_done]

            if not pending:
                print("  ✓ All streams already loaded (use --replace to reload)")
            else:
                print(f"Parsing {len(pending):,} activity files (workers={args.workers}) …")
                total = 0
                for batch in stream_loader.parse_activity_files(
                    pending, workers=args.workers, limit=args.limit
                ):
                    total += db.append_streams(batch)
                db.log_ingestion(
                    "activity_streams", total, str(ACTIVITIES_DIR),
                    f"{len(pending)} files parsed"
                )
                print(f"  ✓ {total:,} stream points")
        else:
            print("\n  (Streams not loaded — add --streams to parse FIT/GPX files)")

        elapsed = time.perf_counter() - t0
        print(f"\n✓ Done in {elapsed:.1f}s")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"\n✗ Failed: {exc}", file=sys.stderr)
        raise
