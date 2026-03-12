SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS activities (
    activity_id             BIGINT PRIMARY KEY,
    activity_date           TIMESTAMPTZ,
    activity_name           VARCHAR,
    activity_type           VARCHAR,
    activity_description    VARCHAR,
    elapsed_time_s          INTEGER,
    moving_time_s           INTEGER,
    distance_km             DOUBLE,
    distance_m              DOUBLE,
    max_speed_ms            DOUBLE,
    avg_speed_ms            DOUBLE,
    avg_elapsed_speed_ms    DOUBLE,
    elevation_gain_m        DOUBLE,
    elevation_loss_m        DOUBLE,
    elevation_low_m         DOUBLE,
    elevation_high_m        DOUBLE,
    max_grade_pct           DOUBLE,
    avg_grade_pct           DOUBLE,
    max_heart_rate_bpm      INTEGER,
    avg_heart_rate_bpm      DOUBLE,
    max_watts_w             DOUBLE,
    avg_watts_w             DOUBLE,
    weighted_avg_power_w    DOUBLE,
    max_cadence_rpm         DOUBLE,
    avg_cadence_rpm         DOUBLE,
    calories_kcal           DOUBLE,
    relative_effort         DOUBLE,
    perceived_exertion      DOUBLE,
    perceived_relative_effort DOUBLE,
    training_load           DOUBLE,
    intensity               DOUBLE,
    total_steps             DOUBLE,
    dirt_distance_m         DOUBLE,
    grade_adjusted_distance_m DOUBLE,
    avg_grade_adjusted_pace DOUBLE,
    media_count             INTEGER,
    bike_name               VARCHAR,
    gear_name               VARCHAR,
    athlete_weight_kg       DOUBLE,
    bike_weight_kg          DOUBLE,
    commute                 BOOLEAN,
    flagged                 BOOLEAN,
    from_upload             BOOLEAN,
    filename                VARCHAR,
    weather_obs_time        TIMESTAMPTZ,
    weather_condition       VARCHAR,
    weather_temp_c          DOUBLE,
    apparent_temp_c         DOUBLE,
    dewpoint_c              DOUBLE,
    humidity_pct            DOUBLE,
    pressure_hpa            DOUBLE,
    wind_speed_ms           DOUBLE,
    wind_gust_ms            DOUBLE,
    wind_bearing_deg        DOUBLE,
    precip_intensity        DOUBLE,
    precip_probability      DOUBLE,
    precip_type             VARCHAR,
    cloud_cover_pct         DOUBLE,
    visibility_km           DOUBLE,
    uv_index                DOUBLE,
    weather_ozone           DOUBLE,
    sunrise_time            TIMESTAMPTZ,
    sunset_time             TIMESTAMPTZ,
    moon_phase              DOUBLE
);

CREATE TABLE IF NOT EXISTS activity_streams (
    activity_id     BIGINT NOT NULL,
    ts              TIMESTAMPTZ NOT NULL,
    sequence        INTEGER,
    lat             DOUBLE,
    lon             DOUBLE,
    altitude_m      DOUBLE,
    distance_m      DOUBLE,
    heart_rate_bpm  INTEGER,
    power_w         INTEGER,
    cadence_rpm     INTEGER,
    speed_ms        DOUBLE,
    temperature_c   DOUBLE,
    source          VARCHAR
);

CREATE INDEX IF NOT EXISTS idx_streams_activity ON activity_streams (activity_id);
CREATE INDEX IF NOT EXISTS idx_streams_ts       ON activity_streams (ts);

CREATE TABLE IF NOT EXISTS saved_routes (
    route_name  VARCHAR PRIMARY KEY,
    filename    VARCHAR
);

CREATE TABLE IF NOT EXISTS saved_route_points (
    route_name  VARCHAR NOT NULL,
    sequence    INTEGER NOT NULL,
    lat         DOUBLE,
    lon         DOUBLE,
    elevation_m DOUBLE,
    PRIMARY KEY (route_name, sequence)
);

CREATE TABLE IF NOT EXISTS bikes (
    bike_name           VARCHAR PRIMARY KEY,
    brand               VARCHAR,
    model               VARCHAR,
    default_sport_types VARCHAR[]
);

CREATE TABLE IF NOT EXISTS ingestion_log (
    table_name    VARCHAR,
    run_at        TIMESTAMPTZ DEFAULT now(),
    rows_inserted INTEGER,
    source_file   VARCHAR,
    notes         VARCHAR
);
"""
