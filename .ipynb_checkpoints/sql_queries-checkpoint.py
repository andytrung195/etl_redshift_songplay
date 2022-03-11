import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS event_staging;"
staging_songs_table_drop = "DROP TABLE IF EXISTS song_staging;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS event_staging (
    artist varchar, 
    auth varchar, 
    first_name varchar, 
    gender varchar, 
    item_session int, 
    last_name varchar, 
    length decimal, 
    level varchar, 
    location varchar, 
    method varchar, 
    page varchar, 
    registration decimal, 
    session_id int, 
    song varchar, 
    status int, 
    ts bigint, 
    user_agent varchar, 
    user_id int);
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS song_staging (
    num_songs int,
    artist_id varchar,
    artist_latitude decimal,
    artist_longitude decimal,
    artist_location varchar,
    artist_name varchar,
    song_id varchar,
    title varchar,
    duration decimal,
    year int
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplay (
    songplay_id int IDENTITY(0,1) PRIMARY KEY SORTKEY DISTKEY,
    start_time timestamp,
    user_id int, 
    level varchar, 
    song_id varchar, 
    artist_id varchar, 
    session_id int, 
    location varchar, 
    user_agent varchar
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
    user_id int PRIMARY KEY SORTKEY,
    first_name varchar,
    last_name varchar,
    gender varchar,
    level varchar)
    diststyle all;
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
    song_id varchar PRIMARY KEY SORTKEY,
    title varchar,
    artist_id varchar,
    year int,
    duration decimal)
    diststyle all;
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
    artist_id varchar PRIMARY KEY SORTKEY,
    name varchar,
    location varchar,
    latitude decimal,
    longitude decimal)
    diststyle all;
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
    start_time timestamp PRIMARY KEY SORTKEY,
    hour int,
    day int,
    week int,
    month int,
    year int,
    weekday varchar)
    diststyle all;
""")

# STAGING TABLES
staging_events_copy = ("""
    copy {} from 's3://udacity-dend/log_data' format as json 'auto'
    credentials 'aws_iam_role={}'
    region 'us-west-2';
""").format('event_staging', config['IAM_ROLE']['ARN'])

staging_songs_copy = ("""
    copy {} from 's3://udacity-dend/song_data' format as json 'auto'
    credentials 'aws_iam_role={}'
    region 'us-west-2';
""").format('song_staging',config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT 
(timestamp 'epoch' + e.ts/1000 * interval '1 second') AS start_time,
e.user_id,
e.level,
s.song_id,
s.artist_id,
e.session_id,
e.location,
e.user_agent
FROM event_staging e
JOIN song_staging s
ON e.song = s.title
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT
user_id, first_name, last_name, gender, level
FROM event_staging
WHERE user_id IS NOT NULL;
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT song_id, title, artist_id, year, duration
FROM song_staging;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT artist_id, artist_name as name, artist_location as location, artist_latitude as latitude, artist_longitude as longitude
FROM song_staging;
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT
(timestamp 'epoch' + ts/1000 * interval '1 second') AS start_time,
EXTRACT (hrs from start_time),
EXTRACT (d from start_time),
EXTRACT (w from start_time),
EXTRACT (mons from start_time),
EXTRACT (yrs from start_time),
EXTRACT (dow from start_time)
FROM event_staging;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

check_stl_load_error = "select * from stl_load_errors;"
check_event_staging = "select * from event_staging limit 20;"
check_song_staging = "select * from song_staging limit 20;"
# test = "SELECT * FROM songplay WHERE songplay_id = 1;"
test = "SELECT * FROM time LIMIT 10;"

