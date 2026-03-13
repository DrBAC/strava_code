"""DuckDB implementation of DatabaseBackend."""

from pathlib import Path

import duckdb
import pandas as pd

from ingestion.config import DB_PATH
from ingestion.db.base import DatabaseBackend
from ingestion.db.schema_duckdb import SCHEMA_SQL


class DuckDBBackend(DatabaseBackend):

    def __init__(self, db_path: str | Path | None = None):
        path = str(db_path or DB_PATH)
        self._conn = duckdb.connect(path)

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def create_schema(self) -> None:
        self._conn.execute(SCHEMA_SQL)

    def close(self) -> None:
        self._conn.close()

    # ── Read ──────────────────────────────────────────────────────────────────

    def query(self, sql: str, params: list | None = None) -> pd.DataFrame:
        if params:
            return self._conn.execute(sql, params).df()
        return self._conn.execute(sql).df()

    def already_loaded_stream_ids(self) -> set[int]:
        rows = self._conn.execute(
            "SELECT DISTINCT activity_id FROM activity_streams"
        ).fetchall()
        return {row[0] for row in rows}

    def get_activity_files(self) -> list[tuple[int, str]]:
        rows = self._conn.execute(
            "SELECT activity_id, filename FROM activities WHERE filename IS NOT NULL"
        ).fetchall()
        return [(int(r[0]), str(r[1])) for r in rows]

    # ── Write ─────────────────────────────────────────────────────────────────

    def upsert_activities(self, df: pd.DataFrame) -> int:
        self._conn.execute(
            "DELETE FROM activities WHERE activity_id IN (SELECT activity_id FROM df)"
        )
        self._conn.execute("INSERT INTO activities BY NAME SELECT * FROM df")
        return len(df)

    def upsert_routes(
        self,
        routes_df: pd.DataFrame,
        points_df: pd.DataFrame,
    ) -> tuple[int, int]:
        route_names = routes_df["route_name"].tolist()
        for name in route_names:
            self._conn.execute(
                "DELETE FROM saved_route_points WHERE route_name = ?", [name]
            )
            self._conn.execute(
                "DELETE FROM saved_routes WHERE route_name = ?", [name]
            )

        self._conn.execute("INSERT INTO saved_routes BY NAME SELECT * FROM routes_df")
        self._conn.execute(
            "INSERT INTO saved_route_points BY NAME SELECT * FROM points_df"
        )
        return len(routes_df), len(points_df)

    def upsert_bikes(self, df: pd.DataFrame) -> int:
        self._conn.execute(
            "DELETE FROM bikes WHERE bike_name IN (SELECT bike_name FROM df)"
        )
        self._conn.execute("INSERT INTO bikes BY NAME SELECT * FROM df")
        return len(df)

    def append_streams(
        self, records: list[dict], table: str = "activity_streams"
    ) -> int:
        if not records:
            return 0
        df = pd.DataFrame(records)
        df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
        self._conn.execute(f"INSERT INTO {table} BY NAME SELECT * FROM df")
        return len(df)

    def log_ingestion(
        self, table: str, rows: int, source: str, notes: str
    ) -> None:
        self._conn.execute(
            "INSERT INTO ingestion_log VALUES (?, now(), ?, ?, ?)",
            [table, rows, source, notes],
        )
