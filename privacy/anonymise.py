"""Core GPS privacy anonymisation logic.

Two-step process applied to each activity's stream:
  1. Trim  — remove GPS points within `radius_m` metres of home at the
             start AND end of the track.
  2. Snap  — if an anchor (lat, lon) is configured for this activity type,
             replace the lat/lon of the first and last surviving GPS points
             with the anchor coordinates. All other fields are unchanged.

Results are written to `activity_streams_private`. Original data is untouched.
"""

import logging
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd
from tqdm import tqdm

if TYPE_CHECKING:
    from ingestion.db.base import DatabaseBackend

logger = logging.getLogger(__name__)

# Ordered column list matching the activity_streams schema.
# Used to ensure consistent column ordering before writing.
STREAM_COLUMNS = (
    "activity_id", "ts", "sequence", "lat", "lon",
    "altitude_m", "distance_m", "heart_rate_bpm",
    "power_w", "cadence_rpm", "speed_ms", "temperature_c", "source",
)

_EARTH_RADIUS_M = 6_371_000.0
_MIN_SURVIVING_POINTS = 2
_PRIVATE_TABLE = "activity_streams_private"


# ── Geometry ──────────────────────────────────────────────────────────────────

def haversine_distances_m(
    lats: np.ndarray,
    lons: np.ndarray,
    ref_lat: float,
    ref_lon: float,
) -> np.ndarray:
    """Vectorised haversine distance (metres) from each point to a reference."""
    lats_r = np.radians(lats)
    lons_r = np.radians(lons)
    ref_lat_r = np.radians(ref_lat)
    ref_lon_r = np.radians(ref_lon)

    dlat = lats_r - ref_lat_r
    dlon = lons_r - ref_lon_r

    a = (np.sin(dlat / 2) ** 2
         + np.cos(ref_lat_r) * np.cos(lats_r) * np.sin(dlon / 2) ** 2)
    return _EARTH_RADIUS_M * 2 * np.arcsin(np.sqrt(a))


# ── Trimming ──────────────────────────────────────────────────────────────────

def trim_activity(
    df: pd.DataFrame,
    radius_m: float,
    home_lat: float,
    home_lon: float,
) -> pd.DataFrame | None:
    """Strip points within `radius_m` of home from the start and end of track.

    Returns the trimmed DataFrame, or None if the entire activity lies within
    the home zone (e.g. a short walk to the letterbox).
    """
    gps_mask = df["lat"].notna() & df["lon"].notna()

    # Build a distance array the same length as df.
    # Rows without GPS get distance=0 (treated as inside the zone).
    distances = np.zeros(len(df), dtype=float)
    if gps_mask.any():
        gps_idx = gps_mask.values
        distances[gps_idx] = haversine_distances_m(
            df.loc[gps_mask, "lat"].values,
            df.loc[gps_mask, "lon"].values,
            home_lat,
            home_lon,
        )

    outside = distances >= radius_m

    if not outside.any():
        return None  # entire activity within home zone

    head_cut = int(np.argmax(outside))          # first outside point
    tail_cut = len(outside) - 1 - int(np.argmax(outside[::-1]))  # last outside point

    if (tail_cut - head_cut + 1) < _MIN_SURVIVING_POINTS:
        return None

    return df.iloc[head_cut : tail_cut + 1].copy()


# ── Anchor snapping ───────────────────────────────────────────────────────────

def snap_anchor(
    df: pd.DataFrame,
    anchor: tuple[float, float],
) -> pd.DataFrame:
    """Replace lat/lon of the first and last GPS-valid rows with `anchor`.

    All other columns (HR, altitude, timestamps, etc.) are unchanged.
    """
    result = df.copy()
    gps_rows = result.index[result["lat"].notna()]

    if len(gps_rows) == 0:
        return result  # nothing to snap

    result.at[gps_rows[0], "lat"] = anchor[0]
    result.at[gps_rows[0], "lon"] = anchor[1]
    result.at[gps_rows[-1], "lat"] = anchor[0]
    result.at[gps_rows[-1], "lon"] = anchor[1]

    return result


# ── Per-activity orchestration ────────────────────────────────────────────────

def process_activity(
    activity_id: int,
    activity_type: str,
    backend: "DatabaseBackend",
    home_lat: float,
    home_lon: float,
) -> int:
    """Anonymise one activity and write to activity_streams_private.

    Returns the number of rows written (0 if skipped).
    """
    from privacy.config import get_zone  # imported here so module loads without env vars

    df = backend.query(
        "SELECT * FROM activity_streams WHERE activity_id = ? ORDER BY sequence",
        [activity_id],
    )

    if df.empty:
        return 0

    zone = get_zone(activity_type)
    trimmed = trim_activity(df, zone["radius_m"], home_lat, home_lon)

    if trimmed is None:
        logger.info("Activity %s (%s) entirely within home zone — skipped", activity_id, activity_type)
        return 0

    if zone["anchor"] is not None:
        trimmed = snap_anchor(trimmed, zone["anchor"])

    # Ensure column order matches the schema before writing
    cols_present = [c for c in STREAM_COLUMNS if c in trimmed.columns]
    trimmed = trimmed[cols_present]

    backend.append_streams(trimmed.to_dict("records"), table=_PRIVATE_TABLE)
    return len(trimmed)


# ── Top-level entry point ─────────────────────────────────────────────────────

def anonymise_all(
    backend: "DatabaseBackend",
    activity_ids: list[int] | None = None,
    replace: bool = False,
) -> dict[str, int]:
    """Anonymise activity streams and write to activity_streams_private.

    Args:
        backend:      Open database backend (DuckDB or PostgreSQL).
        activity_ids: Optional list of specific IDs to process.
                      If None, all activities with stream data are processed.
        replace:      If True, truncate activity_streams_private first.

    Returns:
        Summary dict with keys: processed, written, skipped.
    """
    from privacy.config import HOME_LAT, HOME_LON  # loaded lazily for testability

    if replace:
        backend.query(f"DELETE FROM {_PRIVATE_TABLE}")

    # Fetch (activity_id, activity_type) pairs
    if activity_ids:
        placeholders = ", ".join("?" * len(activity_ids))
        activities_df = backend.query(
            f"SELECT activity_id, activity_type FROM activities"
            f" WHERE activity_id IN ({placeholders})",
            activity_ids,
        )
    else:
        activities_df = backend.query(
            "SELECT DISTINCT a.activity_id, a.activity_type"
            " FROM activities a"
            " JOIN activity_streams s USING (activity_id)"
        )

    if not replace:
        done_ids = backend.query(
            f"SELECT DISTINCT activity_id FROM {_PRIVATE_TABLE}"
        )["activity_id"].tolist()
        activities_df = activities_df[~activities_df["activity_id"].isin(done_ids)]

    if activities_df.empty:
        print(f"  ✓ Nothing to process (use --replace to re-anonymise)")
        return {"processed": 0, "written": 0, "skipped": 0}

    written = 0
    skipped = 0

    with tqdm(total=len(activities_df), unit="activity", desc="Anonymising") as bar:
        for row in activities_df.itertuples(index=False):
            n = process_activity(
                int(row.activity_id),  # type: ignore[arg-type]
                str(row.activity_type),  # type: ignore[arg-type]
                backend,
                HOME_LAT,
                HOME_LON,
            )
            if n == 0:
                skipped += 1
            else:
                written += n
            bar.update(1)

    processed = len(activities_df)
    backend.log_ingestion(
        _PRIVATE_TABLE, written, "activity_streams",
        f"{processed} activities processed, {skipped} skipped (within home zone)",
    )
    return {"processed": processed, "written": written, "skipped": skipped}
