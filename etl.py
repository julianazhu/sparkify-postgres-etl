"""
This module defines and ETL pipeline that imports data song and log
data from .json files under the /data/ directory into a star-schema Postgres database.

The resulting database 'sparkifydb' has the following structure:

#####################    FACT TABLE:     ####################
songplays   - records in log data associated with song plays

##################### DIMENSION TABLES: #####################
users       - users in the app
songs       - songs in music database
artists     - artists in music database
time        - timestamps of records in songplays broken down into specific units
"""

import os
import glob
import io
import json

import pandas as pd

from sql_queries import *
from db_connection import *

# JSON INPUT DIRECTORIES
SONG_DATA_PATH = "data/song_data"
LOG_DATA_PATH = "data/log_data"

# JSON INPUT FILE FIELDS
SONG_FIELDS = ['song_id', 'title', 'artist_id', 'year', 'duration']
ARTIST_FIELDS = ['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
TIME_FIELDS = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
USER_FIELDS = ['userId', 'firstName', 'lastName', 'gender', 'level']
SONGPLAY_FIELDS = ['ts', 'userId', 'level', 'song_id', 'artist_id', 'sessionId', 'location', 'userAgent']

# DB TABLE COLUMNS
SONGPLAY_TABLE_COLS = ['start_time', 'user_id', 'level', 'song_id', 'artist_id', 'session_id', 'location', 'user_agent']


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
    """
    files = get_files(dir_path)

    json_data = []
    for file in files:
        with open(file, "r") as f:
            lines = [line for line in f.readlines() if line.strip()]
            for line in lines:
                line_dict = validate_json(line)
                if line_dict:
                    json_data.append(line_dict)

    return pd.DataFrame(json_data)


def validate_json(json_data):
    """ Check that the .json file is in a valid format """
    try:
        return json.loads(json_data)
    except json.decoder.JSONDecodeError:
        print(f"Invalid JSON, ignoring: \"{json_data}\"")
        return None


def get_songplay_db_data(songplay_log_data, db_conn):
    """
    Returns the songplay data with artist_id
    and song_id from db
    """
    dimensions_dict = retrieve_songplay_dimension_fields(songplay_log_data, db_conn)
    dimensions_df = pd.DataFrame(dimensions_dict)
    songplay_data = pd.concat([songplay_log_data, dimensions_df], axis=1)
    return songplay_data


def transform_log_data(log_data, db_conn):
    """
    Returns relevant songplay & time data from log data
    """
    songplay_log_data = filter_songplays(log_data)

    time_data = transform_time_data(songplay_log_data)
    songplay_data = get_songplay_db_data(songplay_log_data, db_conn)
    return time_data, songplay_data


def clean_data(df, primary_key=None):
    """ Drops null & duplicate rows """
    if primary_key:
        df = df.dropna(subset=[primary_key])
        df = df.drop_duplicates(subset=[primary_key], keep='first')

    df = df.dropna(how='all')
    return df


def filter_songplays(df):
    """ Returns songplay rows """
    return df.loc[df['page'] == "NextSong"]


def transform_time_data(df):
    """
    Return a dataframe with each timestamp in
    the input dataframe extrapolated into units:

    timestamp, hour, day, week of year, month, year, day of week
    """
    timestamps = df.ts

    time_data = []
    for index, ts in timestamps.items():
        dt = pd.to_datetime(ts, unit='ms')
        time_data.append([ts, dt.hour, dt.day, dt.week, dt.month, dt.year, dt.dayofweek])

    time_df = pd.DataFrame(time_data, columns=TIME_FIELDS, dtype='float64')
    return time_df


def retrieve_songplay_dimension_fields(df, db_conn):
    """
    Retrieves the song_id and artist_id fields
    from the db for each songplay row in the input.
    """
    dimensions_dict = {'song_id': [], 'artist_id': []}

    for index, row in df.iterrows():
        results = db_conn.execute_select_query(song_select, (row.song, row.artist, row.length))

        if results:
            song_id, artist_id = results
        else:
            song_id, artist_id = None, None

        dimensions_dict['song_id'].append(song_id)
        dimensions_dict['artist_id'].append(song_id)

    return dimensions_dict


def get_cleaned_data_slice(df, cols, primary_key=None):
    """
    Returns selected columns from dataframe after
    removing null and duplicate rows
    """
    df = df[cols]
    df = clean_data(df, primary_key)
    return df


def get_df_as_file(df, float_format='%.f'):
    """ Returns a string buffer of the dataframe """
    string_buffer = io.StringIO()
    df.to_csv(
        string_buffer,
        sep='\t',
        na_rep='Unknown',
        index=False,
        header=False,
        float_format=float_format
    )

    string_buffer.seek(0)
    return string_buffer


def load_df_to_db(db_conn, insert_query, df):
    """
    Iterates through each row of a dataframe
    and inserts it into the DB
    """
    for _index, row in df.iterrows():
        db_conn.execute_insert_query(insert_query, row)


def bulk_copy_df_to_db(db_conn, df, cols, table, table_cols):
    """
    Transforms a dataframe into a cleaned file and
    copies it as a bulk load into the DB
    """
    df = get_cleaned_data_slice(df, cols)
    df_file = get_df_as_file(df)
    db_conn.execute_copy_from(df_file, table, table_cols)


def main():
    with DbConnection() as db_conn:
        song_data, log_data = extract_song_and_log_data()
        time_data, songplay_data = transform_log_data(log_data, db_conn)

        song_table_data = get_cleaned_data_slice(song_data, SONG_FIELDS, 'song_id')
        load_df_to_db(db_conn, song_table_insert, song_table_data)

        artist_table_data = get_cleaned_data_slice(song_data, ARTIST_FIELDS, 'artist_id')
        load_df_to_db(db_conn, artist_table_insert, artist_table_data)

        time_table_data = get_cleaned_data_slice(time_data, TIME_FIELDS, 'start_time')
        load_df_to_db(db_conn, time_table_insert, time_table_data)

        user_table_data = get_cleaned_data_slice(songplay_data, USER_FIELDS, 'userId')
        load_df_to_db(db_conn, user_table_insert, user_table_data)

        bulk_copy_df_to_db(db_conn, songplay_data, SONGPLAY_FIELDS, 'songplays', SONGPLAY_TABLE_COLS)


if __name__ == "__main__":
    main()
