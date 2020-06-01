# sparkify-etl-pipeline
### Project Overview
This is the first project in the 
[Udacity Data Engineer Nanodegree](https://www.udacity.com/course/data-engineer-nanodegree--nd027).

The virtual startup 'Sparkify' provides a music streaming service. 
In this project we create an ETL pipeline using song and log datasets to a DB optimized for queries on 
analysis of song plays, which could be used to support music recommendations, user behaviour preduction, or 
reporting for artist remuneration.

## SparkifyDB (Star Schema)
#### Fact Table
**songplays** - records in log data associated with song plays i.e. records with page `NextSong`
- _songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent_

#### Dimension Tables
**users** - users in the app
- _user_id, first_name, last_name, gender, level_

**songs** - songs in music database
- _song_id, title, artist_id, year, duration_

**artists** - artists in music database
- _artist_id, name, location, latitude, longitude_

**time** - timestamps of records in songplays broken down into specific units
- _start_time, hour, day, week, month, year, weekday_
