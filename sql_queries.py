import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
staging_events_table_drop = "DROP table IF EXISTS staging_events;"
staging_songs_table_drop = "DROP table IF EXISTS staging_songs;"
songplay_table_drop = "DROP table IF EXISTS songplay;"
user_table_drop = "DROP table IF EXISTS users;"
song_table_drop = "DROP table IF EXISTS song;"
artist_table_drop = "DROP table IF EXISTS artist;"
time_table_drop = "DROP table IF EXISTS time;"



# CREATE TABLES
staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
staging_event_key int IDENTITY(0,1) PRIMARY KEY,
artist            text,
auth              text,
firstName         text,
gender            text,
iteminSession     int,
lastName          text,
length            float,
level             text,
location          text,
method            text,
page              text,
registration      float,
sessionId         int,
song              text,
status            int,
ts                bigint,
userAgent         text,
userId            text
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
num_songs         int PRIMARY KEY,
artist_id         text,
artist_latitude   float,
artist_longitude  float,
artist_location   text,
artist_name       text,
song_id           text,
title             text,
duration          float,
year              int
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay (
songplay_id int IDENTITY(0,1) PRIMARY KEY,
start_time  timestamp NOT NULL, 
user_id     text NOT NULL, 
level       text,
song_id     text, 
artist_id   text, 
session_id  text, 
location    text, 
user_agent  text
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
userId      text primary key,
firstName   text, 
lastName    text, 
gender      text, 
level       text
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS song (
song_id   text primary key,
title     text, 
artist_id text, 
year      int, 
duration  float
);
""")


artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artist (
artist_id        text primary key,
artist_name      text, 
artist_location  text, 
artist_latitude  float,
artist_longitude float
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
start_time timestamp primary key,
hour       int, 
day        int, 
week       int, 
month      int, 
year       int, 
weekday    int
);
""")

# STAGING TABLES
staging_events_copy = ("""
    copy staging_events 
    from {}
    iam_role {}
    format as json {}
""").format(config['S3']['LOG_DATA'],config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    copy staging_songs 
    from {} 
    iam_role {}
    json 'auto'
""").format(config['S3']['SONG_DATA'],config['IAM_ROLE']['ARN'])

# FINAL TABLES
songplay_table_insert = ("""
INSERT INTO songplay (
start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
SELECT 
    DATEADD(s, events.ts/1000, '19700101') AS start_time, 
    events.userId as user_Id, 
    events.level,  
    songs.song_id, 
    songs.artist_id, 
    events.sessionId AS session_id, 
    events.location, 
    events.userAgent AS user_agent
FROM staging_events events
JOIN staging_songs songs
ON (events.song = songs.title
AND events.length = songs.duration 
AND events.artist = songs.artist_name)
WHERE events.page='NextSong'
;
""")

user_table_insert = ("""
INSERT INTO users (
userId, firstName, lastName, gender, level) 
SELECT
    userId,
    firstName,
    lastName,
    gender,
    level
FROM staging_events
;
""")

song_table_insert = ("""
INSERT INTO song (
song_id, title, artist_id, year, duration)
SELECT
    DISTINCT song_id,
    title,
    artist_id,
    year,
    duration
FROM staging_songs
;
""")

artist_table_insert = ("""
INSERT INTO artist (
artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
SELECT
    DISTINCT artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
FROM staging_songs 
;
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT(DATEADD(s, ts/1000, '19700101')) AS start_time,
	   EXTRACT(hour from start_time) AS hour,
       EXTRACT(day from start_time) AS day,
       EXTRACT(week from start_time) AS week,
       EXTRACT(month from start_time) AS month,
       EXTRACT(year from start_time) AS year,
       EXTRACT(weekday from start_time) AS weekday
FROM staging_events;
""")

## QUERY LISTS
#CREATE TABLES QUERIES
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

#ETL QUERIES
copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [user_table_insert, artist_table_insert, song_table_insert, songplay_table_insert,time_table_insert]
