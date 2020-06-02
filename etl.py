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

# Global variables
db_conn = DbConnection()


def get_files(filepath):
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    return all_files


def import_data_from_file(filepath):
    """ Currently only gets first line """
    f = open(filepath, "r")
    return f.readline()


def validate_data(json_data):
    data_dict = json.loads(json_data)
    return data_dict


def create_dataframe(data_dict):
    df = pd.DataFrame([data_dict])
    return df.head(1)


def extract_data_from_df(df, cols):
    """ Currently only gets first row """
    df = df[cols]
    return df.values[0].tolist()


def import_data_from_directory(dir_path):
    """ Currently only gets first file """
    files = get_files(dir_path)
    json_data = import_data_from_file(files[0])
    validated_data = validate_data(json_data)
    return create_dataframe(validated_data)


def import_data():
    song_data = import_data_from_directory(SONG_DATA_PATH)
    log_data = import_data_from_directory(LOG_DATA_PATH)
    return song_data, log_data


def transform_data_for_loading(song_data, log_data):
    songplay_log_data = filter_song_plays(log_data)
    time_data = extract_time_data(songplay_log_data)
    songplay_data = retrieve_songplay_dimension_fields(songplay_log_data)
    return time_data, songplay_data


def load_data_to_table(df, query, cols):
    relevant_data = extract_data_from_df(df, cols)
    db_conn.execute_insert_query(relevant_data, query)


def filter_song_plays(df):
    return df.loc[df['page'] == "NextSong"]


def extract_time_data(df):
    """ Currently only works with one row """
    timestamps = df.ts

    time_data = []
    for index, timestamp in timestamps.items():
        dt = pd.to_datetime(timestamp, unit='ms')
        time_data.append([timestamp, dt.hour, dt.day, dt.week, dt.month, dt.year, dt.dayofweek])

    time_data_dict = dict(zip(TIME_FIELDS, time_data[0]))
    time_df = pd.DataFrame([time_data_dict])
    return time_df


def retrieve_songplay_dimension_fields(df):
    """ Currently only works with one row """
    song_ids = []
    artist_ids = []
    for index, row in df.iterrows():
        results = db_conn.execute_select_query(song_select, (row.song, row.artist, row.length))

        if results:
            song_id, artist_id = results
        else:
            song_id, artist_id = None, None
        song_ids.append(song_id)
        artist_ids.append(artist_id)

    dimensions_dict = {'song_id': song_ids, 'artist_id': artist_ids}
    dimensions_df = pd.DataFrame(dimensions_dict)
    songplay_data = pd.concat([df, dimensions_df], axis=1)
    return songplay_data


def main():
    db_conn.open_connection()

    song_data, log_data = import_data()
    time_data, songplay_data = transform_data_for_loading(song_data, log_data)

    load_data_to_table(song_data, song_table_insert, SONG_FIELDS)
    load_data_to_table(song_data, artist_table_insert, ARTIST_FIELDS)
    load_data_to_table(time_data, time_table_insert, TIME_FIELDS)
    load_data_to_table(songplay_data, user_table_insert, USER_FIELDS)
    load_data_to_table(songplay_data, songplay_table_insert, SONGPLAY_FIELDS)

    db_conn.close_connection()


if __name__ == "__main__":
    main()
