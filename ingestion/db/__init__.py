from ingestion.db.duckdb_backend import DuckDBBackend
from ingestion.db.postgres_backend import PostgreSQLBackend
from ingestion.db.base import DatabaseBackend


def get_backend(name: str, **kwargs) -> DatabaseBackend:
    """Factory: get_backend('duckdb', db_path='strava.duckdb')
                get_backend('postgres', dsn='postgresql://user:pass@host/db')
    """
    if name == "duckdb":
        return DuckDBBackend(**kwargs)
    if name == "postgres":
        return PostgreSQLBackend(**kwargs)
    raise ValueError(f"Unknown backend {name!r}. Choose 'duckdb' or 'postgres'.")
