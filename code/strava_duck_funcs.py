
import pandas as pd
import duckdb
from fitparse import FitFile
import os
import gzip

# --- CONFIGURATION ---

DB_FILE = '../outputs/strava_export.duckdb'

FIT_FILES_FOLDER = '/Users/bac/Documents/PYTHON_PROJECTS/Strava_Analysis/strava_data_dumps/STRAVA+export_8029714/activities/'


# Connect to DuckDB
con = duckdb.connect(DB_FILE)

# --- HELPER FUNCTIONS ---

def create_tables():
    """Create activities and fit_logs tables (DuckDB compatible)."""
    con.execute("DROP TABLE IF EXISTS activities")
    con.execute("DROP TABLE IF EXISTS fit_logs")

    con.execute("""
        CREATE TABLE activities (
                
            ACTIVITY_ID BIGINT, 
            FILENAME TEXT, 
            FILE_SUFFIX TEXT, 
            ACTIVITY_DATE TIMESTAMP, 
            ACTIVITY_NAME TEXT, 
            ACTIVITY_TYPE TEXT, 
            CALORIES DOUBLE, 
            AVERAGE_HEART_RATE DOUBLE, 
            MAX_HEART_RATE INT, 
            ELAPSED_TIME_1 BIGINT, 
            MOVING_TIME INT, 
            DISTANCE_1 DOUBLE, 
            MAX_SPEED DOUBLE, 
            AVERAGE_SPEED DOUBLE, 
            AVERAGE_ELAPSED_SPEED DOUBLE,
            ELEVATION_GAIN INT, 
            ELEVATION_LOSS INT, 
            ELEVATION_LOW INT, 
            ELEVATION_HIGH INT, 
            MAX_GRADE DOUBLE, 
            AVERAGE_GRADE DOUBLE
        )
    """)

    con.execute("""
        CREATE TABLE fit_logs (
                
            activity_id BIGINT,
            timestamp TIMESTAMP,
            long DOUBLE,
            lat DOUBLE,
            distance DOUBLE,
            enhanced_altitude DOUBLE,
                speed DOUBLE,
                gps_accuracy DOUBLE,
                heart_rate DOUBLE
        )
    """)

    print("✅ Tables created fresh.")


def bulk_insert_activities(csv_path):
    """Load activities from CSV, assign activity_ids, and bulk insert."""
    
    df = pd.read_csv(csv_path)

    df['Activity Date'] = pd.to_datetime(df['Activity Date'], format='%b %d, %Y, %I:%M:%S %p')
    df.columns = df.columns.str.upper().str.replace(' ', '_')
    df.columns = df.columns.str.upper().str.replace('.', '_')

    df['FILE_SUFFIX'] = df['FILENAME'].str[-6:].str.replace('\d.', '', regex=True)

    df = df[['ACTIVITY_ID', 'FILENAME','FILE_SUFFIX', 'ACTIVITY_DATE', 'ACTIVITY_NAME', 'ACTIVITY_TYPE', 
         'CALORIES', 'AVERAGE_HEART_RATE', 'MAX_HEART_RATE', 'ELAPSED_TIME_1', 'MOVING_TIME', 'DISTANCE_1', 'MAX_SPEED', 'AVERAGE_SPEED', 'AVERAGE_ELAPSED_SPEED',
        'ELEVATION_GAIN', 'ELEVATION_LOSS', 'ELEVATION_LOW', 'ELEVATION_HIGH', 'MAX_GRADE', 'AVERAGE_GRADE']]


    con.register('temp_activities', df)

    con.execute("""
                INSERT INTO activities (ACTIVITY_ID, FILENAME, FILE_SUFFIX, ACTIVITY_DATE, ACTIVITY_NAME, ACTIVITY_TYPE, 
        CALORIES, AVERAGE_HEART_RATE, MAX_HEART_RATE, ELAPSED_TIME_1, MOVING_TIME, DISTANCE_1, MAX_SPEED, AVERAGE_SPEED, AVERAGE_ELAPSED_SPEED,
        ELEVATION_GAIN, ELEVATION_LOSS, ELEVATION_LOW, ELEVATION_HIGH, MAX_GRADE, AVERAGE_GRADE)
        
                SELECT ACTIVITY_ID, FILENAME, FILE_SUFFIX, ACTIVITY_DATE, ACTIVITY_NAME, ACTIVITY_TYPE, 
        CALORIES, AVERAGE_HEART_RATE, MAX_HEART_RATE, ELAPSED_TIME_1, MOVING_TIME, DISTANCE_1, MAX_SPEED, AVERAGE_SPEED, AVERAGE_ELAPSED_SPEED,
        ELEVATION_GAIN, ELEVATION_LOSS, ELEVATION_LOW, ELEVATION_HIGH, MAX_GRADE, AVERAGE_GRADE 
        
        FROM temp_activities
        """)
    

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
                except Exception as e:
                    print(e)
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

    #next_id = get_next_id('fit_logs', 'log_id')
    #df_logs.insert(0, 'log_id', range(next_id, next_id + len(df_logs)))

    con.register('temp_fit_logs', df_logs)

    con.execute("""
        INSERT INTO fit_logs (activity_id, timestamp, long, lat, distance, enhanced_altitude, speed, gps_accuracy, heart_rate)
        SELECT activity_id, timestamp, long, lat, distance, enhanced_altitude, speed, gps_accuracy, heart_rate
        FROM temp_fit_logs
    """)

    print(f"✅ Inserted {len(df_logs)} logs starting at ID meh")

