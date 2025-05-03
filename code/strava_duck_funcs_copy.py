
import pandas as pd
import os
import gzip
import duckdb
from fitparse import FitFile


# --- CONFIGURATION ---

DB_FILE = '../outputs/strava_test_2.duckdb'

FIT_FILES_FOLDER = '/Users/bac/Documents/PYTHON_PROJECTS/Strava_Analysis/strava_data_dumps/STRAVA+export_8029714/activities/'

IMPORTANT_FIELDS = ['distance','enhanced_altitude','gps_accuracy','enhanced_speed','heart_rate', 'speed']

# Connect to DuckDB
con = duckdb.connect(DB_FILE)

# --- HELPER FUNCTIONS ---

def create_tables():
    """Create activities and fit_logs tables (DuckDB compatible)."""
    con.execute("DROP TABLE IF EXISTS fit_logs")

    con.execute("""
        CREATE TABLE fit_logs (
            log_id BIGINT,
            activity_id BIGINT,
            timestamp TIMESTAMP,
            field_name TEXT,
            field_value DOUBLE
        )
    """)

    print("✅ Tables created fresh.")

    

def parse_fit_file(filepath):

    """Parse a .fit file into a list of records."""
    entries = []
    try:
        fitfile = FitFile(filepath)
        for record in fitfile.get_messages('record'):
            data = {d.name: d.value for d in record}
            data['activity_id'] = str(file)[:-7]
            entries.append(data)
            df = pd.DataFrame(entries)
    
    except Exception as e:
        print(f"⚠️ Failed to parse {filepath}: {e}")

    return df

def build_all_fit_logs():

    """Parse all .fit files into one big DataFrame of logs."""

    all_logs = []
    
    for root, dirs, files in os.walk(FIT_FILES_FOLDER):

        for file in files:

            if file.endswith('.fit.gz'):
                file_path = os.path.join(root, file)
                print(f'fit file! {file_path}')
                
                
                with gzip.open(file_path, 'rb') as f:
                    
                    logs = parse_fit_file(f) # USE THE FITFILE PARSER FUNCTION ABOVE!!!


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

# def test_query():
#     """Query activities joined with fit_logs."""
#     df = con.execute("""
#         SELECT a.name, f.timestamp, f.field_name, f.field_value
#         FROM activities a
#         JOIN fit_logs f ON a.activity_id = f.activity_id
#         ORDER BY a.activity_id, f.timestamp
#         LIMIT 10
#     """).fetchdf()
#     print("\n🎯 Sample joined data:")
#     print(df)
