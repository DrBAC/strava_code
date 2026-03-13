SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS activities (
    activity_id             BIGINT PRIMARY KEY,
    activity_date           TIMESTAMPTZ,
    activity_name           TEXT,
    activity_type           TEXT,
    activity_description    TEXT,
    elapsed_time_s          INTEGER,
    moving_time_s           INTEGER,
    distance_km             DOUBLE PRECISION,
    distance_m              DOUBLE PRECISION,
    max_speed_ms            DOUBLE PRECISION,
    avg_speed_ms            DOUBLE PRECISION,
    avg_elapsed_speed_ms    DOUBLE PRECISION,
    elevation_gain_m        DOUBLE PRECISION,
    elevation_loss_m        DOUBLE PRECISION,
    elevation_low_m         DOUBLE PRECISION,
    elevation_high_m        DOUBLE PRECISION,
    max_grade_pct           DOUBLE PRECISION,
    avg_grade_pct           DOUBLE PRECISION,
    max_heart_rate_bpm      INTEGER,
    avg_heart_rate_bpm      DOUBLE PRECISION,
    max_watts_w             DOUBLE PRECISION,
    avg_watts_w             DOUBLE PRECISION,
    weighted_avg_power_w    DOUBLE PRECISION,
    max_cadence_rpm         DOUBLE PRECISION,
    avg_cadence_rpm         DOUBLE PRECISION,
    calories_kcal           DOUBLE PRECISION,
    relative_effort         DOUBLE PRECISION,
    perceived_exertion      DOUBLE PRECISION,
    perceived_relative_effort DOUBLE PRECISION,
    training_load           DOUBLE PRECISION,
    intensity               DOUBLE PRECISION,
    total_steps             DOUBLE PRECISION,
    dirt_distance_m         DOUBLE PRECISION,
    grade_adjusted_distance_m DOUBLE PRECISION,
    avg_grade_adjusted_pace DOUBLE PRECISION,
    media_count             INTEGER,
    bike_name               TEXT,
    gear_name               TEXT,
    athlete_weight_kg       DOUBLE PRECISION,
    bike_weight_kg          DOUBLE PRECISION,
    commute                 BOOLEAN,
    flagged                 BOOLEAN,
    from_upload             BOOLEAN,
    filename                TEXT,
    weather_obs_time        TIMESTAMPTZ,
    weather_condition       TEXT,
    weather_temp_c          DOUBLE PRECISION,
    apparent_temp_c         DOUBLE PRECISION,
    dewpoint_c              DOUBLE PRECISION,
    humidity_pct            DOUBLE PRECISION,
    pressure_hpa            DOUBLE PRECISION,
    wind_speed_ms           DOUBLE PRECISION,
    wind_gust_ms            DOUBLE PRECISION,
    wind_bearing_deg        DOUBLE PRECISION,
    precip_intensity        DOUBLE PRECISION,
    precip_probability      DOUBLE PRECISION,
    precip_type             TEXT,
    cloud_cover_pct         DOUBLE PRECISION,
    visibility_km           DOUBLE PRECISION,
    uv_index                DOUBLE PRECISION,
    weather_ozone           DOUBLE PRECISION,
    sunrise_time            TIMESTAMPTZ,
    sunset_time             TIMESTAMPTZ,
    moon_phase              DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS activity_streams (
    activity_id     BIGINT NOT NULL,
    ts              TIMESTAMPTZ NOT NULL,
    sequence        INTEGER,
    lat             DOUBLE PRECISION,
    lon             DOUBLE PRECISION,
    altitude_m      DOUBLE PRECISION,
    distance_m      DOUBLE PRECISION,
    heart_rate_bpm  INTEGER,
    power_w         INTEGER,
    cadence_rpm     INTEGER,
    speed_ms        DOUBLE PRECISION,
    temperature_c   DOUBLE PRECISION,
    source          TEXT
);

CREATE INDEX IF NOT EXISTS idx_streams_activity ON activity_streams (activity_id);
CREATE INDEX IF NOT EXISTS idx_streams_ts       ON activity_streams (ts);

CREATE TABLE IF NOT EXISTS activity_streams_private (
    activity_id     BIGINT NOT NULL,
    ts              TIMESTAMPTZ NOT NULL,
    sequence        INTEGER,
    lat             DOUBLE PRECISION,
    lon             DOUBLE PRECISION,
    altitude_m      DOUBLE PRECISION,
    distance_m      DOUBLE PRECISION,
    heart_rate_bpm  INTEGER,
    power_w         INTEGER,
    cadence_rpm     INTEGER,
    speed_ms        DOUBLE PRECISION,
    temperature_c   DOUBLE PRECISION,
    source          TEXT
);

CREATE INDEX IF NOT EXISTS idx_private_streams_activity ON activity_streams_private (activity_id);
CREATE INDEX IF NOT EXISTS idx_private_streams_ts       ON activity_streams_private (ts);

CREATE TABLE IF NOT EXISTS saved_routes (
    route_name  TEXT PRIMARY KEY,
    filename    TEXT
);

CREATE TABLE IF NOT EXISTS saved_route_points (
    route_name  TEXT NOT NULL,
    sequence    INTEGER NOT NULL,
    elevation_m DOUBLE PRECISION,
    lat         DOUBLE PRECISION,
    lon         DOUBLE PRECISION,
    PRIMARY KEY (route_name, sequence)
);

CREATE TABLE IF NOT EXISTS bikes (
    bike_name           TEXT PRIMARY KEY,
    brand               TEXT,
    model               TEXT,
    default_sport_types TEXT[]
);

CREATE TABLE IF NOT EXISTS ingestion_log (
    table_name    TEXT,
    run_at        TIMESTAMPTZ DEFAULT now(),
    rows_inserted INTEGER,
    source_file   TEXT,
    notes         TEXT
);
"""
