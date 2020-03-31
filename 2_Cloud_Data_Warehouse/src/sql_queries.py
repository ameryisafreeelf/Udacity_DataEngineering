import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS fact_songplays;"
user_table_drop = "DROP TABLE IF EXISTS dim_user;"
song_table_drop = "DROP TABLE IF EXISTS dim_song CASCADE;"
artist_table_drop = "DROP TABLE IF EXISTS dim_artist;"
time_table_drop = "DROP TABLE IF EXISTS dim_time;"

# CREATE TABLES

staging_events_table_create= (""" CREATE TABLE staging_events(
    artist          VARCHAR,
    auth            VARCHAR,
    firstName       VARCHAR,
    gender          VARCHAR,
    itemInSession   INTEGER,
    lastName        VARCHAR,
    length          FLOAT,
    level           VARCHAR,
    location        VARCHAR,
    method          VARCHAR,
    page            VARCHAR,
    registration    BIGINT,
    sessionId       INTEGER,
    song            VARCHAR,
    status          INTEGER,
    ts              TIMESTAMP,
    userAgent       VARCHAR,
    userId          INTEGER 
);
""")

staging_songs_table_create = (""" CREATE TABLE IF NOT EXISTS staging_songs(
    song_id             VARCHAR,
    num_songs           INTEGER,
    title               VARCHAR,
    artist_name         VARCHAR,
    artist_latitude     FLOAT,
    year                INTEGER,
    duration            FLOAT,
    artist_id           VARCHAR,
    artist_longitude    FLOAT,
    artist_location     VARCHAR
);
""")

songplay_table_create = (""" CREATE TABLE fact_songplays(
    songplay_id     INTEGER IDENTITY(0,1) PRIMARY KEY SORTKEY,
    start_time      TIMESTAMP,
    user_id         INTEGER,
    level           VARCHAR,
    song_id         VARCHAR,
    artist_id       VARCHAR, 
    session_id      INTEGER, 
    location        VARCHAR, 
    user_agent      VARCHAR
);
""")

user_table_create = (""" CREATE TABLE dim_user(
    user_id         INTEGER PRIMARY KEY, 
    first_name      VARCHAR, 
    last_name       VARCHAR, 
    gender          VARCHAR,
    level           VARCHAR
);
""")

song_table_create = (""" CREATE TABLE dim_song(
    song_id         VARCHAR PRIMARY KEY DISTKEY,
    title           VARCHAR,
    artist_id       VARCHAR DISTKEY,
    year            INTEGER, 
    duration        FLOAT,
    FOREIGN KEY(artist_id) REFERENCES dim_artist(artist_id)
);
""")

artist_table_create = (""" CREATE TABLE dim_artist(
    artist_id       VARCHAR PRIMARY KEY DISTKEY,
    name            VARCHAR,
    location        VARCHAR,
    latitude        FLOAT,
    longitude       FLOAT
);
""")

time_table_create = (""" CREATE TABLE dim_time(
    start_time     TIMESTAMP PRIMARY KEY SORTKEY DISTKEY,
    hour           INTEGER,
    day            INTEGER,
    week           INTEGER,
    month          INTEGER,
    year           INTEGER,
    weekday        INTEGER
);
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {}
    credentials 'aws_iam_role={}'
    region 'us-west-2' format as JSON {}
    timeformat as 'epochmillisecs';
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    copy staging_songs from {}
    credentials 'aws_iam_role={}'
    region 'us-west-2' format as JSON 'auto';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO fact_songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT
    DISTINCT S_EVT.ts      AS start_time,
    S_EVT.userId           AS user_id,
    S_EVT.level            AS level,
    S_SNG.song_id          AS song_id,
    S_SNG.artist_id        AS artist_id,
    S_EVT.sessionId        AS session_id,
    S_EVT.location         AS location,
    S_EVT.userAgent        AS user_agent
FROM staging_events S_EVT
JOIN staging_songs S_SNG ON (S_EVT.song = S_SNG.title AND S_EVT.artist = S_SNG.artist_name)
WHERE S_EVT.page  =  'NextSong';
""")

user_table_insert = ("""
INSERT INTO dim_user(user_id, first_name, last_name, gender, level)
SELECT  
    DISTINCT userId     AS user_id,
    firstName           AS first_name,
    lastName            AS last_name,
    gender              AS gender,
    level               AS level
FROM staging_events
WHERE user_id IS NOT NULL;
""")

song_table_insert = ("""
INSERT INTO dim_song(song_id, title, artist_id, year, duration)
SELECT
    DISTINCT S_SNG.song_id,
    S_SNG.title,
    S_SNG.artist_id,
    S_SNG.year,
    S_SNG.duration
FROM staging_songs S_SNG
WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO dim_artist(artist_id, name, location, latitude, longitude)
SELECT
   DISTINCT artist_id    AS artist_id,
   artist_name           AS name,
   artist_location       AS location,
   artist_latitude       AS latitude,
   artist_longitude      AS longitude
FROM staging_songs
WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
INSERT INTO dim_time(start_time, hour, day, week, month, year, weekday)
SELECT  
    DISTINCT staging_events.ts                 AS start_time,
    EXTRACT(hour FROM staging_events.ts)       AS hour,
    EXTRACT(day FROM staging_events.ts)        AS day,
    EXTRACT(week FROM staging_events.ts)       AS week,
    EXTRACT(month FROM staging_events.ts)      AS month,
    EXTRACT(year FROM staging_events.ts)       AS year,
    EXTRACT(dayofweek FROM staging_events.ts)  as weekday
FROM staging_events
WHERE start_time IS NOT NULL;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, artist_table_create, song_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
# insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
insert_table_queries = [song_table_insert]
