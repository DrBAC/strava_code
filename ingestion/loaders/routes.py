"""Load saved routes (routes.csv + GPX) and bikes — no database logic."""

import pandas as pd

from ingestion.config import BIKES_CSV, EXPORT_DIR, ROUTES_CSV


def load_routes() -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
    """Return (routes_df, {route_name: points_df}).

    routes_df has columns: route_name, filename
    Each points_df has columns: route_name, sequence, lat, lon, elevation_m
    """
    try:
        import gpxpy
    except ImportError:
        raise ImportError("gpxpy is required: pip install gpxpy")

    routes_df = pd.read_csv(ROUTES_CSV)
    routes_df.columns = ["route_name", "filename"]

    points_by_route: dict[str, pd.DataFrame] = {}

    for _, row in routes_df.iterrows():
        gpx_path = EXPORT_DIR / row["filename"]
        if not gpx_path.exists():
            print(f"  ⚠ Missing GPX: {gpx_path}")
            continue

        with open(gpx_path) as f:
            gpx = gpxpy.parse(f)

        points = []
        seq = 0
        for track in gpx.tracks:
            for segment in track.segments:
                for pt in segment.points:
                    points.append({
                        "route_name":  row["route_name"],
                        "sequence":    seq,
                        "lat":         pt.latitude,
                        "lon":         pt.longitude,
                        "elevation_m": pt.elevation,
                    })
                    seq += 1

        if points:
            points_by_route[row["route_name"]] = pd.DataFrame(points)

    return routes_df, points_by_route


def load_bikes() -> pd.DataFrame:
    """Return bikes DataFrame with default_sport_types as Python lists."""
    df = pd.read_csv(BIKES_CSV)
    df.columns = ["bike_name", "brand", "model", "default_sport_types_raw"]
    df["default_sport_types"] = df["default_sport_types_raw"].apply(
        lambda v: v.split("|") if isinstance(v, str) and v.strip() else []
    )
    return df.drop(columns=["default_sport_types_raw"])
