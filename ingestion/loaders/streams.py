"""Parse FIT and GPX activity files into stream records — no database logic.

FIT files  (.fit.gz)  → parsed with fitparse (Garmin binary format)
GPX files  (.gpx)     → parsed with gpxpy (XML GPS tracks)

FIT lat/lon are stored in semicircles:
    degrees = semicircles * (180 / 2**31)
"""

import gzip
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from tqdm import tqdm

from ingestion.config import ACTIVITIES_DIR

_SEMICIRCLES_TO_DEGREES = 180.0 / (2**31)


# ── FIT ───────────────────────────────────────────────────────────────────────

def _parse_fit(activity_id: int, path: Path) -> list[dict]:
    try:
        import fitparse
    except ImportError:
        raise ImportError("fitparse is required: pip install fitparse")

    rows: list[dict] = []
    try:
        with gzip.open(path, "rb") as gz:
            fitfile = fitparse.FitFile(gz)
            for i, msg in enumerate(fitfile.get_messages("record")):
                fields = {k: v for k, v in msg.get_values().items() if v is not None}
                lat = fields.get("position_lat")
                lon = fields.get("position_long")
                rows.append({
                    "activity_id":    activity_id,
                    "ts":             fields.get("timestamp"),
                    "sequence":       i,
                    "lat":            lat * _SEMICIRCLES_TO_DEGREES if lat is not None else None,
                    "lon":            lon * _SEMICIRCLES_TO_DEGREES if lon is not None else None,
                    "altitude_m":     fields.get("altitude"),
                    "distance_m":     fields.get("distance"),
                    "heart_rate_bpm": fields.get("heart_rate"),
                    "power_w":        fields.get("power"),
                    "cadence_rpm":    fields.get("cadence"),
                    "speed_ms":       fields.get("speed"),
                    "temperature_c":  fields.get("temperature"),
                    "source":         "fit",
                })
    except Exception as exc:
        print(f"\n  ⚠ FIT parse error {path.name}: {exc}")
    return rows


# ── GPX ───────────────────────────────────────────────────────────────────────

def _parse_gpx(activity_id: int, path: Path) -> list[dict]:
    try:
        import gpxpy
    except ImportError:
        raise ImportError("gpxpy is required: pip install gpxpy")

    rows: list[dict] = []
    try:
        with open(path) as f:
            gpx = gpxpy.parse(f)
        i = 0
        for track in gpx.tracks:
            for segment in track.segments:
                for pt in segment.points:
                    rows.append({
                        "activity_id":    activity_id,
                        "ts":             pt.time,
                        "sequence":       i,
                        "lat":            pt.latitude,
                        "lon":            pt.longitude,
                        "altitude_m":     pt.elevation,
                        "distance_m":     None,
                        "heart_rate_bpm": None,
                        "power_w":        None,
                        "cadence_rpm":    None,
                        "speed_ms":       None,
                        "temperature_c":  None,
                        "source":         "gpx",
                    })
                    i += 1
    except Exception as exc:
        print(f"\n  ⚠ GPX parse error {path.name}: {exc}")
    return rows


def _parse_one(activity_id: int, filename: str) -> list[dict]:
    path = ACTIVITIES_DIR / Path(filename).name
    if not path.exists():
        return []
    if path.suffix == ".gz":
        return _parse_fit(activity_id, path)
    if path.suffix == ".gpx":
        return _parse_gpx(activity_id, path)
    return []


# ── Public generator ──────────────────────────────────────────────────────────

_FLUSH_EVERY = 100_000  # yield a batch after accumulating this many records


def parse_activity_files(
    tasks: list[tuple[int, str]],
    workers: int = 4,
    limit: int | None = None,
) -> Iterator[list[dict]]:
    """Parse activity files in parallel and yield batches of stream records.

    Args:
        tasks:   [(activity_id, filename), ...]
        workers: Thread pool size.
        limit:   Cap on number of files to process (useful for testing).

    Yields:
        Batches of stream record dicts (up to ~_FLUSH_EVERY records each).
    """
    if limit:
        tasks = tasks[:limit]

    if not tasks:
        return

    batch: list[dict] = []

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(_parse_one, aid, fname): (aid, fname)
            for aid, fname in tasks
        }
        with tqdm(total=len(futures), unit="file", desc="Streams") as bar:
            for future in as_completed(futures):
                records = future.result()
                batch.extend(records)
                bar.update(1)
                bar.set_postfix(points=f"{len(batch):,}")

                if len(batch) >= _FLUSH_EVERY:
                    yield batch
                    batch = []

    if batch:
        yield batch
