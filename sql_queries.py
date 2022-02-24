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
    artist varchar NOT NULL, 
    auth varchar NOT NULL, 
    first_name varchar NOT NULL, 
    gender varchar NOT NULL, 
    item_session int NOT NULL, 
    last_name varchar NOT NULL, 
    length decimal NOT NULL, 
    level varchar NOT NULL, 
    location varchar NOT NULL, 
    method varchar NOT NULL, 
    page varchar NOT NULL, 
    registration decimal NOT NULL, 
    session_id int NOT NULL, 
    song varchar NOT NULL, 
    status int NOT NULL, 
    ts timestamp NOT NULL, 
    user_agent varchar NOT NULL, 
    user_id int NOT NULL)
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS song_staging (
    num_songs int NOT NULL,
    artist_id varchar,
    artist_latitude decimal,
    artist_longitude decimal,
    artist_location varchar,
    artist_name varchar,
    song_id varchar,
    title varchar NOT NULL,
    duration decimal NOT NULL,
    year int NOT NULL
    )
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplay (
    songplay_id SERIAL PRIMARY KEY SORTKEY DISTKEY,
    start_time timestamp,
    user_id int, 
    level varchar, 
    song_id varchar, 
    artist_id varchar, 
    session_id, 
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
    copy {} from 's3://udacity-dend/log_data' 
    credentials 'aws_iam_role={}'
    gzip region 'us-west-2';
""").format('event_staging', config.get('IAM_ROLE','ARN'))

staging_songs_copy = ("""
    copy {} from 's3://s3://udacity-dend/song_data' 
    credentials 'aws_iam_role={}'
    gzip region 'us-west-2';
""").format('song_staging',config.get('IAM_ROLE','ARN'))

# FINAL TABLES

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

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
