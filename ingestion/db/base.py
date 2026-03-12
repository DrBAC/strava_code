"""Abstract base class for database backends."""

from abc import ABC, abstractmethod

import pandas as pd


class DatabaseBackend(ABC):

    @abstractmethod
    def create_schema(self) -> None:
        """Create all tables (idempotent)."""

    @abstractmethod
    def close(self) -> None:
        """Close the database connection."""

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    # ── Read ──────────────────────────────────────────────────────────────────

    @abstractmethod
    def query(self, sql: str, params: list | None = None) -> pd.DataFrame:
        """Execute a SELECT query and return a DataFrame.
        Use ? as the placeholder character for both backends.
        """

    @abstractmethod
    def already_loaded_stream_ids(self) -> set[int]:
        """Return set of activity_ids that already have stream rows."""

    @abstractmethod
    def get_activity_files(self) -> list[tuple[int, str]]:
        """Return [(activity_id, filename), ...] for all activities with a file."""

    # ── Write ─────────────────────────────────────────────────────────────────

    @abstractmethod
    def upsert_activities(self, df: pd.DataFrame) -> int:
        """Insert/replace activities. Returns row count."""

    @abstractmethod
    def upsert_routes(
        self,
        routes_df: pd.DataFrame,
        points_df: pd.DataFrame,
    ) -> tuple[int, int]:
        """Insert/replace saved routes and their GPS points.
        Returns (routes_count, points_count).
        """

    @abstractmethod
    def upsert_bikes(self, df: pd.DataFrame) -> int:
        """Insert/replace bikes. Returns row count."""

    @abstractmethod
    def append_streams(self, records: list[dict]) -> int:
        """Bulk-insert stream records. Returns row count."""

    @abstractmethod
    def log_ingestion(
        self, table: str, rows: int, source: str, notes: str
    ) -> None:
        """Append an audit entry to ingestion_log."""
