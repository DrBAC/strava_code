"""CLI entry point for GPS privacy anonymisation.

Reads from activity_streams, applies home-zone trimming and optional
anchor-point snapping, and writes results to activity_streams_private.

Usage:
    python -m privacy.run                              # DuckDB, all activities
    python -m privacy.run --backend postgres
    python -m privacy.run --replace                    # re-anonymise from scratch
    python -m privacy.run --activity-id 12345678       # single activity
    python -m privacy.run --activity-id 111 --activity-id 222
    python -m privacy.run --db /path/to/other.duckdb
    python -m privacy.run --dsn postgresql://user@host/strava

Configure privacy zones and anchor points in privacy/config.py.
Set HOME_LAT and HOME_LON in your .env file before running.
"""

import argparse
import sys
import time

from ingestion.config import DB_PATH, PG_DSN
from ingestion.db import get_backend
from privacy.anonymise import anonymise_all


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Anonymise GPS streams — trim home zone, snap to public anchor",
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
        "--replace",
        action="store_true",
        help="Truncate activity_streams_private before processing",
    )
    parser.add_argument(
        "--activity-id",
        type=int,
        action="append",
        dest="activity_ids",
        default=None,
        metavar="ID",
        help="Process a specific activity ID (can be repeated)",
    )
    args = parser.parse_args()

    kwargs = {"db_path": args.db} if args.backend == "duckdb" else {"dsn": args.dsn}
    t0 = time.perf_counter()
    print(f"\nBackend : {args.backend}")
    print(f"Target  : {args.db if args.backend == 'duckdb' else args.dsn}\n")

    with get_backend(args.backend, **kwargs) as db:
        db.create_schema()  # ensures activity_streams_private exists

        summary = anonymise_all(db, activity_ids=args.activity_ids, replace=args.replace)

    elapsed = time.perf_counter() - t0
    print(
        f"\n✓ Done in {elapsed:.1f}s  —  "
        f"{summary['processed']} processed, "
        f"{summary['written']:,} points written, "
        f"{summary['skipped']} skipped"
    )


if __name__ == "__main__":
    try:
        main()
    except RuntimeError as exc:
        # Catches missing HOME_LAT/HOME_LON with a clear message
        print(f"\n✗ {exc}", file=sys.stderr)
        sys.exit(1)
    except Exception as exc:
        print(f"\n✗ Failed: {exc}", file=sys.stderr)
        raise
