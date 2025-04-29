
import pandas as pd
import os
import glob
import duckdb
from strava_duck_funcs import *
from fitparse import FitFile


# --- CONFIGURATION ---

DB_FILE = 'project_database.duckdb'

FIT_FILES_FOLDER = 'path/to/your/fit_files/'

IMPORTANT_FIELDS = ['heart_rate', 'cadence', 'speed']

# Connect to DuckDB
con = duckdb.connect(DB_FILE)

# --- HELPER FUNCTIONS ---

def create_tables():
    """Create activities and fit_logs tables (DuckDB compatible)."""
    con.execute("DROP TABLE IF EXISTS activities")
    con.execute("DROP TABLE IF EXISTS fit_logs")

    con.execute("""
        CREATE TABLE activities (
            activity_id BIGINT,
            name TEXT,
            date TIMESTAMP,
            distance DOUBLE,
            duration DOUBLE,
            fit_filename TEXT
        )
    """)

    # con.execute("""
    #     CREATE TABLE fit_logs (
    #         log_id BIGINT,
    #         activity_id BIGINT,
    #         timestamp TIMESTAMP,
    #         field_name TEXT,
    #         field_value DOUBLE
    #     )
    # """)
    
    print("✅ Tables created fresh.")

def get_next_id(table_name, id_column):
    """Find the next available ID for a table."""
    result = con.execute(f"SELECT MAX({id_column}) FROM {table_name}").fetchone()[0]
    return 1 if result is None else result + 1

def bulk_insert_activities(csv_path):
    """Load activities from CSV, assign activity_ids, and bulk insert."""
    df = pd.read_csv(csv_path)

    if 'fit_filename' not in df.columns:
        raise ValueError("activities.csv must have a 'fit_filename' column.")

    next_id = get_next_id('activities', 'activity_id')
    df.insert(0, 'activity_id', range(next_id, next_id + len(df)))

    df.to_sql('activities', con, if_exists='append', index=False)
    print(f"✅ Inserted {len(df)} activities starting at ID {next_id}")

def parse_fit_file(filepath):
    """Parse a .fit file into a list of records."""
    try:
        fitfile = FitFile(filepath)
    except Exception as e:
        print(f"⚠️ Failed to parse {filepath}: {e}")
        return []

    entries = []
    for record in fitfile.get_messages('record'):
        timestamp = None
        metrics = []

        for field in record:
            if field.name == 'timestamp':
                timestamp = field.value
            elif field.name in IMPORTANT_FIELDS and field.value is not None:
                metrics.append((field.name, field.value))

        if timestamp:
            for field_name, field_value in metrics:
                entries.append({
                    'timestamp': timestamp,
                    'field_name': field_name,
                    'field_value': field_value
                })

    return entries

def build_all_fit_logs():
    """Parse all .fit files into one big DataFrame of logs."""
    activities_df = pd.read_sql("SELECT activity_id, fit_filename FROM activities", con)

    all_logs = []

    fit_files = glob.glob(os.path.join(FIT_FILES_FOLDER, '*.fit'))

    for filepath in fit_files:
        filename = os.path.basename(filepath)
        match = activities_df[activities_df['fit_filename'] == filename]

        if match.empty:
            print(f"⚠️ No matching activity for {filename}")
            continue

        activity_id = match.iloc[0]['activity_id']
        logs = parse_fit_file(filepath)

        if not logs:
            continue

        for log in logs:
            log['activity_id'] = activity_id

        all_logs.extend(logs)

    print(f"✅ Parsed {len(all_logs)} total log entries from FIT files.")
    return pd.DataFrame(all_logs)

def bulk_insert_fit_logs(df_logs):
    """Bulk insert fit logs with auto-incremented log_ids."""
    if df_logs.empty:
        print("⚠️ No logs to insert.")
        return

    next_id = get_next_id('fit_logs', 'log_id')
    df_logs.insert(0, 'log_id', range(next_id, next_id + len(df_logs)))

    df_logs.to_sql('fit_logs', con, if_exists='append', index=False)
    print(f"✅ Inserted {len(df_logs)} logs starting at ID {next_id}")

def test_query():
    """Query activities joined with fit_logs."""
    df = con.execute("""
        SELECT a.name, f.timestamp, f.field_name, f.field_value
        FROM activities a
        JOIN fit_logs f ON a.activity_id = f.activity_id
        ORDER BY a.activity_id, f.timestamp
        LIMIT 10
    """).fetchdf()
    print("\n🎯 Sample joined data:")
    print(df)