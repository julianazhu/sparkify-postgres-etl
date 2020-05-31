# Standard library imports
import os
import glob

# Third Party imports
import psycopg2
import pandas as pd
import json

# Local application imports
from sql_queries import *

SONG_DATA_PATH = "data/song_data"
SONG_TABLE_COLS = ["song_id", "title", "artist_id", "year", "duration"]


def get_files(filepath):
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    return all_files


def import_data(filepath):
    f = open(filepath, "r")
    return f.read()


def validate_data(json_data):
    data_dict = json.loads(json_data)
    return data_dict


def create_dataframe(data_dict):
    df = pd.DataFrame([data_dict])
    return df.head(1)


def extract_data(df, cols):
    song_df = df[cols]
    return song_df.values[0].tolist()


def load_song_data_to_db(cur, conn, song_df):
    cur.execute(song_table_insert, song_df)
    conn.commit()


def connect_to_db():
    # conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=postgres")
    cur = conn.cursor()
    return cur, conn


def main():
    song_files = get_files(SONG_DATA_PATH)
    json_data = import_data(song_files[0])
    validated_data = validate_data(json_data)
    data_frame = create_dataframe(validated_data)
    song_df = extract_data(data_frame, SONG_TABLE_COLS)
    cur, conn = connect_to_db
    load_song_data_to_db(cur, conn, song_df)


if __name__ == "__main__":
    main()