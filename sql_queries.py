# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS times;"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE songplays(
    songplay_id     integer PRIMARY KEY,
    start_time      timestamp,
    user_id         integer,
    level           text,
    song_id         integer REFERENCES songs(song_id),
    artist_id       integer REFERENCES artists(artist_id),
    session_id      integer,
    location        text,
    user_agent      text
);
""")

user_table_create = ("""
CREATE TABLE users(
    user_id         integer PRIMARY KEY,
    first_name      text,
    last_name       text,
    gender          text,
    level           text
);
""")

song_table_create = ("""
CREATE TABLE songs(
    song_id         integer PRIMARY KEY,
    title           text,
    artist_id       integer REFERENCES artists(artist_id),
    year            integer,
    duration        numeric
);
""")

artist_table_create = ("""
CREATE TABLE artists(
    artist_id       integer PRIMARY KEY,
    name            text,
    location        text,
    latitude        float8,
    longitude       float8
);
""")

time_table_create = ("""
CREATE TABLE times(
    start_time      integer PRIMARY KEY,
    hour            integer,
    day             integer,
    week            integer,
    month           integer,
    year            integer,
    weekday         text
);
""")

# INSERT RECORDS

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")


time_table_insert = ("""
""")

# FIND SONGS

song_select = ("""
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
