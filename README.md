# sparkify-etl-pipeline
## Project Overview
This is the first project in the 
[Udacity Data Engineer Nanodegree](https://www.udacity.com/course/data-engineer-nanodegree--nd027).

The virtual startup 'Sparkify' provides a music streaming service. 
In this project we create an ETL pipeline using song and log datasets to a DB optimized for queries on 
analysis of song plays, which could be used to support music recommendations, user behaviour prediction, or 
reporting for artist remuneration.

## How to Run
Clone this repository and (if you want to use your own data), replace the files in the `/data/song_data`
and `data/log_data` directories with your own song and log data.

```
pip3 install -r requirements.txt
python3 create_tables.py
python3 etl.py
```

**Note:** Expects to be able to connect to Postgres using the following credentials:
```
host=127.0.0.1 dbname=sparkifydb user=student password=student
```

#### Project Files
* **etl.py** - The main script that runs the ETL Pipeline which processes the files in `data/` directory 
* **create_tables.py** - Creates the `sparkifydb`, dropping the existing db & tables if they already exist
* **db_connection.py** - Defines a context manager class `DbConnection` which wraps the `psycopg2` connection & methods
* **tests/test_etl.py** - Test for the ETL Pipeline
* **tests/test.ipynb** - Executes a select on each table to confirm that it is populated.


## SparkifyDB (Star Schema)
###### Fact Table
**songplays** - records in log data associated with song plays i.e. records with page `NextSong`
- _songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent_

###### Dimension Tables
**users** - users in the app
- _user_id, first_name, last_name, gender, level_

**songs** - songs in music database
- _song_id, title, artist_id, year, duration_

**artists** - artists in music database
- _artist_id, name, location, latitude, longitude_

**time** - timestamps of records in songplays broken down into specific units
- _start_time, hour, day, week, month, year, weekday_

## Data Source - The Million Song Dataset
Thierry Bertin-Mahieux, Daniel P.W. Ellis, Brian Whitman, and Paul Lamere.
[The Million Song Dataset](http://millionsongdataset.com/). In Proceedings of the 12th International Society
for Music Information Retrieval Conference (ISMIR 2011), 2011.