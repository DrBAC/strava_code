# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**Data Analysis of Sports** — a personal sports data analysis project built on a Strava data export. The goal is to explore, visualize, and derive performance insights from cycling, running, and walking activities.

## Planned Architecture

The project roadmap (from README) defines these sequential phases:

1. **Data Exploration & Munging** — parse and clean raw Strava export data
2. **Database + ETL** — build a local database and ETL pipeline to load activity data
3. **Dash App** — interactive dashboard for querying training data via the local DB
4. **Cross-correlation** — integrate Apple Health and weather data
5. **Analysis & ML** — performance pattern detection, automatic training classification

**Tech stack**: Python, Pandas, NumPy, Matplotlib, Plotly, Jupyter Notebooks, Dash, OpenStreetMap/GPX

## Data Sources

All raw data lives in `STRAVA+export_8029714/`:

| Path | Description |
|------|-------------|
| `activities.csv` | 1,526 activity records with 70+ columns (speed, HR, power, cadence, elevation, weather, training load, etc.) |
| `activities/*.fit.gz` | 894 binary FIT files (raw device telemetry per activity) |
| `routes/*.gpx` | 10 GPS route files in XML format |
| `media/*.jpg` | 53 activity photos |
| `bikes.csv` | 3 bikes defined (with component history) |
| `profile.csv` | Athlete profile (contains PII — handle carefully) |
| Various `*.csv` | Social data (comments, followers, reactions), goals, segments, equipment |

**FIT files** are binary compressed (`.fit.gz`) and require a library like `fitparse` or `garmin-fit-sdk` to decode. **GPX files** are standard XML and can be parsed with `gpxpy` or `xml.etree`.

## Current State

No Python code or notebooks exist yet. The project is at the raw data stage, ready for development.

## Development Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Always use `.venv/bin/python` (or activate the venv) — never the system Python.

## Database

Supports **DuckDB** (default, file-based) and **PostgreSQL**. Both use an identical schema.

- **DuckDB**: `strava.duckdb` at repo root — no server needed; connects to Tableau via JDBC/ODBC
- **PostgreSQL**: configure via `.env` (copy `.env.example`) or `--dsn` flag

### Ingestion

```bash
# DuckDB (default) — activities + routes + bikes
python -m ingestion.run

# Also parse FIT/GPX files into activity_streams (slow — 894 files)
python -m ingestion.run --streams

# Test streams on a small sample first
python -m ingestion.run --streams --limit 20

# PostgreSQL
python -m ingestion.run --backend postgres
python -m ingestion.run --backend postgres --dsn postgresql://user:pass@localhost/strava

# Full reload
python -m ingestion.run --replace --streams

python -m ingestion.run --help
```

### Architecture

```
ingestion/
  config.py          # file paths + PG_DSN from env
  loaders/           # pure data parsing — no DB imports
    activities.py    #   activities.csv  → DataFrame
    streams.py       #   FIT/GPX files  → record generator
    routes.py        #   routes + bikes → DataFrames
  db/                # database layer
    base.py          #   DatabaseBackend ABC
    duckdb_backend.py
    postgres_backend.py
    schema_duckdb.py
    schema_postgres.py
  run.py             # CLI orchestrator
```

The loaders are DB-agnostic — they can be called and tested independently.
The backends implement the same `DatabaseBackend` ABC; `ingestion.db.get_backend(name)` is the factory.

### Tables

| Table | Rows | Description |
|-------|------|-------------|
| `activities` | 1,516 | One row per activity — all summary metrics, weather, equipment |
| `activity_streams` | ~varies | Time-series GPS/HR/power/cadence at ~1Hz per activity (from FIT/GPX files) |
| `saved_routes` | 10 | Named routes from routes.csv |
| `saved_route_points` | 9,126 | GPS points for each saved route |
| `bikes` | 3 | Equipment registry |
| `ingestion_log` | — | Audit trail of each load run |

### Key schema notes
- `activities.distance_km` = user-preferred units; `activities.distance_m` = SI metres (more precise)
- Speed columns are in **m/s** as exported by Strava
- `activity_streams.source` = `'fit'` or `'gpx'` — FIT files have richer telemetry (power, cadence, HR); GPX files have GPS only

## PII Notes

The export contains personally identifiable information (name, email, GPS coordinates, activity photos, weight/health metrics). Per the README, PII removal code will be developed and shared before any public sharing.
