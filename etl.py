import os
import glob
import io
import json

import pandas as pd

from sql_queries import *
from db_connection import *

# JSON INPUT DIRECTORY PATHS
SONG_DATA_PATH = "data/song_data"
LOG_DATA_PATH = "data/log_data"

# JSON INPUT FILE FIELDS
SONG_FIELDS = ["song_id", "title", "artist_id", "year", "duration"]
ARTIST_FIELDS = ["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]
TIME_FIELDS = ["start_time", "hour", "day", "week", "month", "year", "weekday"]
USER_FIELDS = ["userId", "firstName", "lastName", "gender", "level"]
SONGPLAY_FIELDS = ["ts", "userId", "level", "song_id", "artist_id", "sessionId", "location", "userAgent"]


def extract_song_and_log_data():
    """ Imports song & log data from directories """
    song_data = extract_json_data_from_dir(SONG_DATA_PATH)
    log_data = extract_json_data_from_dir(LOG_DATA_PATH)
    return song_data, log_data


def get_files(filepath):
    """ Returns a list of all .json files in path """
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    return all_files


def extract_json_data_from_dir(dir_path):
    """
    Returns a dataframe containing data
    from all .json files in the directory.

    TODO: Skips the file with error if it is incorrectly formatted
    """
    files = get_files(dir_path)

    json_data = []
    for file in files:
        with open(file, "r") as f:
            lines = [line for line in f.readlines() if line.strip()]
            for line in lines:
                json_data.append(json.loads(line))

    return pd.DataFrame(json_data)


def validate_json(json_data):
    """ Check that the .json file is in a valid format """
    try:
        data_dict = json.loads(json_data)
    except JSONDecodeError:
        print("Invalid JSON\n")
    return data_dict


def get_songplay_data(songplay_log_data, db_conn):
    """ Returns the songplay data with artist_id and song_id from db """
    dimensions_dict = retrieve_songplay_dimension_fields(songplay_log_data, db_conn)
    dimensions_df = pd.DataFrame(dimensions_dict)
    songplay_data = pd.concat([songplay_log_data, dimensions_df], axis=1)
    return songplay_data


def transform_log_data(log_data, db_conn):
    """ Returns cleaned & transformed songplay & time data from log data """
    songplay_log_data = filter_songplays(log_data)
    time_data = transform_time_data(songplay_log_data)
    time_data = deduplicate_on_primary_key(time_data, 'start_time')

    songplay_data = get_songplay_data(songplay_log_data, db_conn)
    songplay_data = clean_data(songplay_data)
    return time_data, songplay_data


def deduplicate_on_primary_key(df, primary_key):
    """ Removes duplicate artists from the song data """
    return df.drop_duplicates(subset=primary_key, keep='first')


def clean_data(df, primary_key=None):
    """ Drops null & duplicate rows """
    if primary_key:
        df = df.dropna(subset=primary_key)
        df = deduplicate_on_primary_key(df, primary_key)

    # Drop row if all elements are null
    df = df.dropna(how='all')
    return df


def get_slice_as_file(df, cols, primary_key=None, float_format='%.2f'):
    """ Insert specified columns into db """
    relevant_data = df[cols]
    cleaned_data = clean_data(relevant_data, primary_key)

    string_buffer = io.StringIO()
    cleaned_data.to_csv(
        string_buffer,
        sep='\t',
        na_rep='Unknown',
        index=False,
        header=False,
        float_format=float_format
    )

    string_buffer.seek(0)
    return string_buffer


def filter_songplays(df):
    """ Returns songplay rows """
    return df.loc[df['page'] == "NextSong"]


def transform_time_data(df):
    """
    Extract timestamp column from a dataframe
    Return a dataframe with each timestamp extrapolated into:
    timestamp, hour, day, week of year, month, year, day of week
    """
    timestamps = df.ts

    time_data = []
    for index, ts in timestamps.items():
        dt = pd.to_datetime(ts, unit='ms')
        time_data.append([ts, dt.hour, dt.day, dt.week, dt.month, dt.year, dt.dayofweek])

    time_df = pd.DataFrame(time_data, columns=TIME_FIELDS)
    return time_df


def retrieve_songplay_dimension_fields(df, db_conn):
    """
    Retrieves the song_id and artist_id fields
    from the db for each songplay.
    """
    dimensions_dict = {'song_id': [], 'artist_id': []}

    for index, row in df.iterrows():
        results = db_conn.execute_select_query(song_select, (row.song, row.artist, row.length))

        if results:
            song_id, artist_id = results
        else:
            song_id, artist_id = None, None

        dimensions_dict["song_id"].append(song_id)
        dimensions_dict["artist_id"].append(song_id)

    return dimensions_dict


def main():
    with DbConnection() as db_conn:
        song_data, log_data = extract_song_and_log_data()
        time_data, songplay_data = transform_log_data(log_data, db_conn)

        song_file = get_slice_as_file(song_data, SONG_FIELDS, ["song_id"])
        db_conn.execute_copy_from(song_file, "songs", SONG_COLS)

        artist_file = get_slice_as_file(song_data, ARTIST_FIELDS, ["artist_id"])
        db_conn.execute_copy_from(artist_file, "artists", ARTIST_COLS)

        time_file = get_slice_as_file(time_data, TIME_FIELDS, ["start_time"])
        db_conn.execute_copy_from(time_file, "times", TIME_COLS)

        user_file = get_slice_as_file(songplay_data, USER_FIELDS, ["userId"])
        db_conn.execute_copy_from(user_file, "users", USER_COLS)

        songplays_file = get_slice_as_file(songplay_data, SONGPLAY_FIELDS, float_format='%.f')
        db_conn.execute_copy_from(songplays_file, "songplays", SONGPLAY_COLS)


if __name__ == "__main__":
    main()
