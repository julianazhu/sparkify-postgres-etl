# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS times;"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE songplays(
    songplay_id     integer PRIMARY KEY,
    start_time      integer REFERENCES times(start_time),
    user_id         integer REFERENCES users(user_id),
    level           text,
    song_id         text REFERENCES songs(song_id),
    artist_id       text REFERENCES artists(artist_id),
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
    song_id         text PRIMARY KEY,
    title           text,
    artist_id       text,
    year            integer,
    duration        numeric
);
""")

artist_table_create = ("""
CREATE TABLE artists(
    artist_id       text PRIMARY KEY,
    name            text,
    location        text,
    latitude        float8,
    longitude       float8
);
""")

time_table_create = ("""
CREATE TABLE times(
    start_time      bigint PRIMARY KEY,
    hour            integer NOT NULL,
    day             integer NOT NULL,
    week            integer NOT NULL,
    month           integer NOT NULL,
    year            integer NOT NULL,
    weekday         text NOT NULL
);
""")

# INSERT RECORDS

songplay_table_insert = ("""
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
VALUES (%s, %s, %s, %s, %s)
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
VALUES (%s, %s, %s, %s, %s)
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
VALUES (%s, %s, %s, %s, %s)
""")


time_table_insert = ("""
INSERT INTO times (start_time, hour, day, week, month, year, weekday)
VALUES (%s, %s, %s, %s, %s, %s, %s)
""")

# FIND SONGS

song_select = ("""
""")

# QUERY LISTS
# Due to the enforcement of foreign key constrains, the songplay table must be created last and dropped first

create_table_queries = [user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
