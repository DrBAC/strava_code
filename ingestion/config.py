import os
from pathlib import Path

# Load .env if python-dotenv is installed (silently skip if not)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

ROOT = Path(__file__).parent.parent
EXPORT_DIR = ROOT / "STRAVA+export_8029714"

# ── DuckDB ────────────────────────────────────────────────────────────────────
DB_PATH = ROOT / "strava.duckdb"

# ── PostgreSQL ────────────────────────────────────────────────────────────────
# Set PG_DSN directly, or set individual PG_* vars, or create a .env file.
PG_DSN = os.getenv("PG_DSN") or (
    "postgresql://{user}:{password}@{host}:{port}/{dbname}".format(
        user=os.getenv("PG_USER", "postgres"),
        password=os.getenv("PG_PASSWORD", ""),
        host=os.getenv("PG_HOST", "localhost"),
        port=os.getenv("PG_PORT", "5432"),
        dbname=os.getenv("PG_DATABASE", "strava"),
    )
)

# ── Source file paths ─────────────────────────────────────────────────────────
ACTIVITIES_CSV = EXPORT_DIR / "activities.csv"
ACTIVITIES_DIR = EXPORT_DIR / "activities"
ROUTES_CSV = EXPORT_DIR / "routes.csv"
ROUTES_DIR = EXPORT_DIR / "routes"
BIKES_CSV = EXPORT_DIR / "bikes.csv"
PROFILE_CSV = EXPORT_DIR / "profile.csv"
