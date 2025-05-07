
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


def semicircles_to_degrees(semicircles):

    """
    Converts lat/long from the fit format of semicircle --> normal lat/longs
    """

    return semicircles * (180 / 2**31)


def parse_fit_file(directory):

    """Parse a .fit file into a list of records."""

    fit_files = []


    for root, dirs, files in os.walk(directory):

        for file in files:

            if file.endswith('.fit.gz'):
                file_path = os.path.join(root, file)
                all_records = []
                with gzip.open(file_path, 'rb') as f:
                    fitfile = FitFile(f)
                    for record in fitfile.get_messages('record'):
                        data = {d.name: d.value for d in record}
                        data['activity_id'] = str(file)[:-7]
                        all_records.append(data)
                # Convert to DataFrame
                df = pd.DataFrame(all_records)
                try:
                    df['lat'] = df['position_lat'].apply(semicircles_to_degrees)
                    df['long'] = df['position_long'].apply(semicircles_to_degrees)
                except:
                    pass
                fit_files.append(df)
                print(f'fit file! {file_path}\n All parsed ')
            
            else:
                file_path = os.path.join(root, file)
                print(f'other file! {file_path}\n -- IGNORING FOR NOW')

    fit_df = pd.concat(fit_files, ignore_index=True)
    
    return fit_df



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
