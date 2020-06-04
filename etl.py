# Standard library imports
import os
import glob

# Third Party imports
import pandas as pd
import json

# Local application imports
from sql_queries import *
from db_connection import *

# Constants
SONG_DATA_PATH = "data/song_data"
LOG_DATA_PATH = "data/log_data"

SONG_FIELDS = ["song_id", "title", "artist_id", "year", "duration"]
ARTIST_FIELDS = ["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]
TIME_FIELDS = ["start_time", "hour", "day", "week", "month", "year", "weekday"]
USER_FIELDS = ["userId", "firstName", "lastName", "gender", "level"]
SONGPLAY_FIELDS = ["length", "userId", "level", "song_id", "artist_id", "sessionId", "location", "userAgent"]


def extract_song_and_log_data():
    """ Imports song and log data from directory .json files """
    song_data = extract_json_data_from_dir(SONG_DATA_PATH)
    log_data = extract_json_data_from_dir(LOG_DATA_PATH)
    return song_data, log_data


def extract_json_data_from_dir(dir_path):
    """
    Returns a dataframe containing the concatenated data
    of all .json files in the directory.

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


def get_files(filepath):
    """ Returns a list of all .json files in the filepath """
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    return all_files


def validate_json(json_data):
    """
    Check that the .json file is in a valid format
    TODO: put something here to validate the data
    """
    data_dict = json.loads(json_data)
    return data_dict


def extract_data_from_df(df, cols):
    """ Currently only gets first row """
    df = df[cols]
    return df.values[0].tolist()


def fetch_songplay_data(songplay_log_data, db_conn):
    """ Returns the songplay data with artist_id and song_id from db """
    dimensions_dict = retrieve_songplay_dimension_fields(songplay_log_data, db_conn)
    dimensions_df = pd.DataFrame(dimensions_dict)
    songplay_data = pd.concat([songplay_log_data, dimensions_df], axis=1)
    return songplay_data


def transform_log_data(log_data, db_conn):
    """ Returns the songplay data & time data from the log data """
    songplay_log_data = filter_song_plays(log_data)
    time_data = transform_time_data(songplay_log_data)

    songplay_data = fetch_songplay_data(songplay_log_data, db_conn)
    return time_data, songplay_data


def get_slice(df, cols):
    """ Insert specified dataframe columns into db """
    return df[cols].values[0].tolist()


def filter_song_plays(df):
    """ Returns only rows related to songplays """
    return df.loc[df['page'] == "NextSong"]


def transform_time_data(df):
    """
    Extract timestamp column from a dataframe
    Return a dataframe with each timestamp extrapolated into:
    timestamp, hour, day, week of year, month, year, day of week
    """
    timestamps = df.ts

    time_data = []
    for index, timestamp in timestamps.items():
        dt = pd.to_datetime(timestamp, unit='ms')
        time_data.append([timestamp, dt.hour, dt.day, dt.week, dt.month, dt.year, dt.dayofweek])

    time_df = pd.DataFrame(time_data, columns=TIME_FIELDS)
    return time_df


def retrieve_songplay_dimension_fields(df, db_conn):
    """
    Retrieves the song_id and artist_id fields
    from the db for each songplay.

    Currently only works with one row
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

        db_conn.execute_insert_query(song_table_insert, get_slice(song_data, SONG_FIELDS))
        db_conn.execute_insert_query(artist_table_insert, get_slice(song_data, ARTIST_FIELDS))

        db_conn.execute_insert_query(time_table_insert, get_slice(time_data, TIME_FIELDS))
        db_conn.execute_insert_query(user_table_insert, get_slice(songplay_data, USER_FIELDS))
        db_conn.execute_insert_query(songplay_table_insert, get_slice(songplay_data, SONGPLAY_FIELDS))


if __name__ == "__main__":
    main()
