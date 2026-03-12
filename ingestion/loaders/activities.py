"""Load activities.csv into a clean DataFrame — no database logic."""

import pandas as pd

from ingestion.config import ACTIVITIES_CSV

# Pandas auto-mangles duplicate column headers to "Col", "Col.1", etc.
# The CSV has five duplicate headers: Elapsed Time, Distance, Max Heart Rate,
# Relative Effort, Commute. The second Distance.1 is in metres (SI); the first
# Distance is in the user's preferred units (km).
# Mapping None = drop the column entirely.
_COLUMN_MAP: dict[str, str | None] = {
    "Activity ID":                  "activity_id",
    "Activity Date":                "activity_date",
    "Activity Name":                "activity_name",
    "Activity Type":                "activity_type",
    "Activity Description":         "activity_description",
    "Elapsed Time":                 "elapsed_time_s",
    "Distance":                     "distance_km",
    "Max Heart Rate":               "max_heart_rate_bpm",
    "Relative Effort":              "relative_effort",
    "Commute":                      "commute",
    "Activity Private Note":        None,
    "Activity Gear":                "gear_name",
    "Filename":                     "filename",
    "Athlete Weight":               "athlete_weight_kg",
    "Bike Weight":                  "bike_weight_kg",
    "Elapsed Time.1":               None,           # duplicate of Elapsed Time
    "Moving Time":                  "moving_time_s",
    "Distance.1":                   "distance_m",   # metres (SI, more precise)
    "Max Speed":                    "max_speed_ms",
    "Average Speed":                "avg_speed_ms",
    "Elevation Gain":               "elevation_gain_m",
    "Elevation Loss":               "elevation_loss_m",
    "Elevation Low":                "elevation_low_m",
    "Elevation High":               "elevation_high_m",
    "Max Grade":                    "max_grade_pct",
    "Average Grade":                "avg_grade_pct",
    "Average Positive Grade":       None,
    "Average Negative Grade":       None,
    "Max Cadence":                  "max_cadence_rpm",
    "Average Cadence":              "avg_cadence_rpm",
    "Max Heart Rate.1":             None,           # duplicate
    "Average Heart Rate":           "avg_heart_rate_bpm",
    "Max Watts":                    "max_watts_w",
    "Average Watts":                "avg_watts_w",
    "Calories":                     "calories_kcal",
    "Max Temperature":              None,
    "Average Temperature":          None,
    "Relative Effort.1":            None,           # duplicate
    "Total Work":                   None,
    "Number of Runs":               None,
    "Uphill Time":                  None,
    "Downhill Time":                None,
    "Other Time":                   None,
    "Perceived Exertion":           "perceived_exertion",
    "Type":                         None,
    "Start Time":                   None,
    "Weighted Average Power":       "weighted_avg_power_w",
    "Power Count":                  None,
    "Prefer Perceived Exertion":    None,
    "Perceived Relative Effort":    "perceived_relative_effort",
    "Commute.1":                    None,           # numeric duplicate of Commute
    "Total Weight Lifted":          None,
    "From Upload":                  "from_upload",
    "Grade Adjusted Distance":      "grade_adjusted_distance_m",
    "Weather Observation Time":     "weather_obs_time",
    "Weather Condition":            "weather_condition",
    "Weather Temperature":          "weather_temp_c",
    "Apparent Temperature":         "apparent_temp_c",
    "Dewpoint":                     "dewpoint_c",
    "Humidity":                     "humidity_pct",
    "Weather Pressure":             "pressure_hpa",
    "Wind Speed":                   "wind_speed_ms",
    "Wind Gust":                    "wind_gust_ms",
    "Wind Bearing":                 "wind_bearing_deg",
    "Precipitation Intensity":      "precip_intensity",
    "Sunrise Time":                 "sunrise_time",
    "Sunset Time":                  "sunset_time",
    "Moon Phase":                   "moon_phase",
    "Bike":                         "bike_name",
    "Gear":                         None,
    "Precipitation Probability":    "precip_probability",
    "Precipitation Type":           "precip_type",
    "Cloud Cover":                  "cloud_cover_pct",
    "Weather Visibility":           "visibility_km",
    "UV Index":                     "uv_index",
    "Weather Ozone":                "weather_ozone",
    "Jump Count":                   None,
    "Total Grit":                   None,
    "Average Flow":                 None,
    "Flagged":                      "flagged",
    "Average Elapsed Speed":        "avg_elapsed_speed_ms",
    "Dirt Distance":                "dirt_distance_m",
    "Newly Explored Distance":      None,
    "Newly Explored Dirt Distance": None,
    "Activity Count":               None,
    "Total Steps":                  "total_steps",
    "Carbon Saved":                 None,
    "Pool Length":                  None,
    "Training Load":                "training_load",
    "Intensity":                    "intensity",
    "Average Grade Adjusted Pace":  "avg_grade_adjusted_pace",
    "Timer Time":                   None,
    "Total Cycles":                 None,
    "Media":                        "media_count",
}

_DATE_FORMAT = "%b %d, %Y, %I:%M:%S %p"  # e.g. "Mar 1, 2015, 5:35:32 PM"


def _parse_bool(series: pd.Series) -> pd.Series:
    return series.map(
        lambda v: True  if str(v).strip().lower() in ("true",  "1", "1.0")
             else False if str(v).strip().lower() in ("false", "0", "0.0")
             else None
    )


def load_activities_df() -> pd.DataFrame:
    """Read activities.csv and return a fully typed, deduplicated DataFrame."""
    df = pd.read_csv(ACTIVITIES_CSV, low_memory=False)

    keep = {raw: clean for raw, clean in _COLUMN_MAP.items()
            if clean is not None and raw in df.columns}
    df = df[list(keep.keys())].rename(columns=keep)

    df["activity_id"] = pd.to_numeric(df["activity_id"], errors="coerce").astype("Int64")
    df["activity_date"] = pd.to_datetime(
        df["activity_date"], format=_DATE_FORMAT, errors="coerce"
    )
    for ts_col in ("weather_obs_time", "sunrise_time", "sunset_time"):
        if ts_col in df.columns:
            df[ts_col] = pd.to_datetime(df[ts_col], errors="coerce")

    for bool_col in ("commute", "flagged", "from_upload"):
        if bool_col in df.columns:
            df[bool_col] = _parse_bool(df[bool_col])

    numeric_cols = [
        "elapsed_time_s", "moving_time_s", "distance_km", "distance_m",
        "max_speed_ms", "avg_speed_ms", "avg_elapsed_speed_ms",
        "elevation_gain_m", "elevation_loss_m", "elevation_low_m", "elevation_high_m",
        "max_grade_pct", "avg_grade_pct",
        "max_heart_rate_bpm", "avg_heart_rate_bpm",
        "max_watts_w", "avg_watts_w", "weighted_avg_power_w",
        "max_cadence_rpm", "avg_cadence_rpm",
        "calories_kcal", "relative_effort", "perceived_exertion",
        "perceived_relative_effort", "training_load", "intensity",
        "total_steps", "dirt_distance_m", "grade_adjusted_distance_m",
        "avg_grade_adjusted_pace", "athlete_weight_kg", "bike_weight_kg",
        "weather_temp_c", "apparent_temp_c", "dewpoint_c", "humidity_pct",
        "pressure_hpa", "wind_speed_ms", "wind_gust_ms", "wind_bearing_deg",
        "precip_intensity", "precip_probability", "cloud_cover_pct",
        "visibility_km", "uv_index", "weather_ozone", "moon_phase",
        "media_count",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["activity_id"])
    df["activity_id"] = df["activity_id"].astype(int)
    return df
