# etl_redshift_songplay

## OverView: This database is for analysis of songplay which is the information of user using the application, song, and artist.
This database has 7 tables: 2 staging table for copying data from json file in s3 bucket, 5 tables: including 1 fact table and 4 dimension tables, getting data from 2 staging table.
### Fact Table: 1.songplays - records in log data associated with song plays i.e. records with page NextSong (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)

### Dimension Table: 2.users - users in the app (user_id, first_name, last_name, gender, level) 3.songs - songs in music database (song_id, title, artist_id, year, duration) 4.artists - artists in music database artist_id, name, location, latitude, longitude 5.time - timestamps of records in songplays broken down into specific units start_time, hour, day, week, month, year, weekday

## How to run Run sql_queries.py => create_tables.py => etl.py
## Explaination of the files :
sql_queries.py : containing sql queries
create_table.py : drop table and create table functions
etl.py : same as etl.ipynb but processing with multiple file