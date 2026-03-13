"""PostgreSQL implementation of DatabaseBackend.

Requires psycopg2-binary. Reads connection settings from a DSN string or
the PG_* environment variables defined in config.py.

Usage:
    backend = PostgreSQLBackend(dsn="postgresql://user:pass@localhost/strava")
    # or rely on PG_DSN from environment / .env file:
    backend = PostgreSQLBackend()
"""

import pandas as pd
import psycopg2
import psycopg2.extras

from ingestion.config import PG_DSN
from ingestion.db.base import DatabaseBackend
from ingestion.db.schema_postgres import SCHEMA_SQL


def _df_to_rows(df: pd.DataFrame) -> list[tuple]:
    """Convert DataFrame to list of tuples, replacing NaN/NaT/NA with None."""
    def _clean(v):
        try:
            return None if pd.isnull(v) else v
        except (TypeError, ValueError):
            return v  # lists and other non-scalar types pass through unchanged

    return [tuple(_clean(v) for v in row) for row in df.itertuples(index=False)]


def _col_list(df: pd.DataFrame) -> str:
    return ", ".join(df.columns)


def _placeholder_list(df: pd.DataFrame) -> str:
    return ", ".join(["%s"] * len(df.columns))


class PostgreSQLBackend(DatabaseBackend):

    def __init__(self, dsn: str | None = None):
        self._conn = psycopg2.connect(dsn or PG_DSN)
        self._conn.autocommit = False

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def create_schema(self) -> None:
        with self._conn.cursor() as cur:
            cur.execute(SCHEMA_SQL)
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    # ── Read ──────────────────────────────────────────────────────────────────

    def query(self, sql: str, params: list | None = None) -> pd.DataFrame:
        """Execute SELECT. Use ? as placeholder — converted to %s for psycopg2."""
        pg_sql = sql.replace("?", "%s")
        with self._conn.cursor() as cur:
            cur.execute(pg_sql, params)
            cols = [d[0] for d in cur.description]
            return pd.DataFrame(cur.fetchall(), columns=cols)

    def already_loaded_stream_ids(self) -> set[int]:
        with self._conn.cursor() as cur:
            cur.execute("SELECT DISTINCT activity_id FROM activity_streams")
            return {row[0] for row in cur.fetchall()}

    def get_activity_files(self) -> list[tuple[int, str]]:
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT activity_id, filename FROM activities WHERE filename IS NOT NULL"
            )
            return [(int(r[0]), str(r[1])) for r in cur.fetchall()]

    # ── Write ─────────────────────────────────────────────────────────────────

    def upsert_activities(self, df: pd.DataFrame) -> int:
        ids = df["activity_id"].tolist()
        rows = _df_to_rows(df)
        cols = _col_list(df)

        with self._conn.cursor() as cur:
            cur.execute(
                "DELETE FROM activities WHERE activity_id = ANY(%s)", [ids]
            )
            psycopg2.extras.execute_values(
                cur,
                f"INSERT INTO activities ({cols}) VALUES %s",
                rows,
                page_size=500,
            )
        self._conn.commit()
        return len(rows)

    def upsert_routes(
        self,
        routes_df: pd.DataFrame,
        points_df: pd.DataFrame,
    ) -> tuple[int, int]:
        route_names = routes_df["route_name"].tolist()

        with self._conn.cursor() as cur:
            cur.execute(
                "DELETE FROM saved_route_points WHERE route_name = ANY(%s)",
                [route_names],
            )
            cur.execute(
                "DELETE FROM saved_routes WHERE route_name = ANY(%s)",
                [route_names],
            )
            psycopg2.extras.execute_values(
                cur,
                f"INSERT INTO saved_routes ({_col_list(routes_df)}) VALUES %s",
                _df_to_rows(routes_df),
            )
            psycopg2.extras.execute_values(
                cur,
                f"INSERT INTO saved_route_points ({_col_list(points_df)}) VALUES %s",
                _df_to_rows(points_df),
                page_size=1000,
            )
        self._conn.commit()
        return len(routes_df), len(points_df)

    def upsert_bikes(self, df: pd.DataFrame) -> int:
        # Convert Python lists to PostgreSQL array literal strings e.g. {"a","b"}
        # psycopg2 execute_values does not auto-adapt Python lists to TEXT[]
        def _to_pg_array(v) -> str:
            if not v:
                return "{}"
            escaped = ['"' + str(s).replace('"', '\\"') + '"' for s in v]
            return "{" + ",".join(escaped) + "}"

        df = df.copy()
        df["default_sport_types"] = df["default_sport_types"].apply(_to_pg_array)

        bike_names = df["bike_name"].tolist()
        with self._conn.cursor() as cur:
            cur.execute(
                "DELETE FROM bikes WHERE bike_name = ANY(%s)", [bike_names]
            )
            psycopg2.extras.execute_values(
                cur,
                f"INSERT INTO bikes ({_col_list(df)}) VALUES %s",
                _df_to_rows(df),
            )
        self._conn.commit()
        return len(df)

    def append_streams(
        self, records: list[dict], table: str = "activity_streams"
    ) -> int:
        if not records:
            return 0
        df = pd.DataFrame(records)
        df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
        rows = _df_to_rows(df)
        cols = _col_list(df)

        with self._conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                f"INSERT INTO {table} ({cols}) VALUES %s",
                rows,
                page_size=5000,
            )
        self._conn.commit()
        return len(rows)

    def log_ingestion(
        self, table: str, rows: int, source: str, notes: str
    ) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                "INSERT INTO ingestion_log (table_name, rows_inserted, source_file, notes)"
                " VALUES (%s, %s, %s, %s)",
                (table, rows, source, notes),
            )
        self._conn.commit()
