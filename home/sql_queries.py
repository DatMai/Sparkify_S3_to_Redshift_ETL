import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
SONG_DATA = config.get('S3', 'SONG_DATA')
LOG_DATA = config.get('S3','LOG_DATA')
LOG_JSONPATH = config.get('S3','LOG_JSONPATH')
ARN = config.get('IAM_ROLE','ARN')
REGION = config.get('S3','REGION')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist VARCHAR(255),
    auth VARCHAR(50),
    firstName VARCHAR(255),
    gender VARCHAR(1),
    itemInSession INT,
    lastName VARCHAR(255),
    length DECIMAL(10, 5),
    level VARCHAR(10),
    location VARCHAR(255),
    method VARCHAR(10),
    page VARCHAR(50),
    registration BIGINT,
    sessionId INT,
    song VARCHAR(255),
    status INT,
    ts TIMESTAMP,
    userAgent VARCHAR(1024),
    userId INT
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    song_id VARCHAR(255),
    num_songs INT,
    artist_id VARCHAR(255),
    artist_latitude DECIMAL(10, 7),
    artist_longitude DECIMAL(10, 7),
    artist_location VARCHAR(255),
    artist_name VARCHAR(255),
    title VARCHAR(255),
    duration DECIMAL(10, 5),
    year INT
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id INT IDENTITY(0,1) PRIMARY KEY,
    start_time TIMESTAMP NOT NULL,
    user_id INT NOT NULL,
    level VARCHAR(10),
    song_id VARCHAR(255) NOT NULL,
    artist_id VARCHAR(255) NOT NULL,
    session_id INT,
    location VARCHAR(255),
    user_agent VARCHAR(1024)
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id INT PRIMARY KEY,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    gender VARCHAR(1),
    level VARCHAR(10)
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR(255) PRIMARY KEY,
    title VARCHAR(255),
    artist_id VARCHAR(255),
    year INT,
    duration DECIMAL(10, 5)
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR(255) PRIMARY KEY,
    name VARCHAR(255),
    location VARCHAR(255),
    latitude DECIMAL(10, 7),
    longitude DECIMAL(10, 7)
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP PRIMARY KEY,
    hour INT,
    day INT,
    week INT,
    month INT,
    year INT,
    weekday INT
);
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events FROM {}
CREDENTIALS 'aws_iam_role={}'
COMPUPDATE OFF REGION '{}'
TIMEFORMAT AS 'epochmillisecs'
FORMAT AS JSON {}
TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(LOG_DATA, ARN, REGION, LOG_JSONPATH)


staging_songs_copy = ("""
COPY staging_songs FROM {}
CREDENTIALS 'aws_iam_role={}'
COMPUPDATE OFF REGION '{}'
FORMAT AS JSON 'auto'
TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(SONG_DATA, ARN, REGION)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT se.ts,
       se.userId      as user_id,
       se.level       as level,
       ss.song_id     as song_id,
       ss.artist_id   as artist_id,
       se.sessionId   as session_id,
       se.location    as location,
       se.userAgent   as user_agent
FROM staging_events se 
JOIN staging_songs ss
ON se.artist = ss.artist_name AND se.song = ss.title
WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT userId      as user_id,
                firstName   as first_name,
                lastName    as last_name,
                gender      as gender,
                level       as level
FROM staging_events
WHERE userId IS NOT NULL AND page = 'NextSong';
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id,
                title,
                artist_id,
                year,
                duration
FROM staging_songs;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id        as artist_id,
                artist_name      as name,
                artist_location  as location,
                artist_latitude  as latitude,
                artist_longitude as longitude
FROM staging_songs
WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT ts                as start_time,
       EXTRACT(HOUR from ts)      as hour,
       EXTRACT(DAY  from ts)      as day,
       EXTRACT(WEEK from ts)      as week,
       EXTRACT(MONTH from ts)     as month,
       EXTRACT(YEAR from ts)      as year,
       EXTRACT(DAYOFWEEK from ts) as weekday
FROM staging_events
WHERE ts IS NOT NULL AND page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
