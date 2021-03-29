import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN=config.get('IAM_ROLE','ARN')
LOG_DATA= config.get('S3','LOG_DATA')
SONG_DATA= config.get('S3','SONG_DATA')
LOG_JSONPATH = config.get('S3','LOG_JSONPATH')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events(
        id bigint identity(1, 1),
        artist text,
        auth text,
        firstName text,
        gender text,
        itemInSession double precision, 
        lastName text,
        length double precision,
        level text,
        location text,
        method text,
        page text,
        registration double precision,
        sessionId double precision sortkey, 
        song text,
        status int,
        ts bigint, 
        userAgent text,
        userId double precision distkey
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs(
        id bigint identity(1, 1),
        artist_id text sortkey,
        artist_latitude double precision,
        artist_location text,
        artist_longitude double precision,
        artist_name text,
        duration double precision,
        num_songs int,
        song_id text,
        title text,
        year int distkey
    )

""")

songplay_table_create = ("""
     CREATE TABLE IF NOT EXISTS songplays (
        id bigint identity(1, 1),
        start_time bigint, 
        user_id int distkey, 
        song_id text, 
        artist_id text,
        level text, 
        session_id int sortkey, 
        location text, 
        user_agent text
    
    )
""")


user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        id bigint identity(1, 1),
        user_id int sortkey, 
        first_name text, 
        last_name text, 
        gender text, 
        level text
    )
    diststyle all;
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        id bigint identity(1, 1),
        song_id text, 
        title text, 
        artist_id text, 
        year int sortkey, 
        duration int
    )
    diststyle all;
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        id bigint identity(1, 1),
        artist_id text, 
        name text, 
        year int sortkey, 
        location text, 
        latitude int, 
        longitude int
    )
    diststyle all;
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        id bigint identity(1, 1),
        start_time text, 
        hour int, 
        day int, 
        week int, 
        month int, 
        year int, 
        weekday int
    )diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {}
    credentials 'aws_iam_role={}'
    format as json 's3://udacity-dend/log_json_path.json'
    compupdate off region 'us-west-2'
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    copy staging_songs from {}
    credentials 'aws_iam_role={}'
    format as json 'auto'
    compupdate off region 'us-west-2';
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT staging_events.ts as start_time, 
    staging_events.userId as user_id, 
    staging_events.level,
    staging_songs.song_id as song_id, 
    staging_songs.artist_id as artist_id, 
    staging_events.sessionId as session_id,
    staging_events.location, 
    staging_events.userAgent as user_agent
    FROM staging_events LEFT JOIN staging_songs
    ON ((staging_songs.artist_name=staging_events.artist  or (staging_songs.artist_name is null and staging_events.artist IS null)) AND (staging_songs.title=staging_events.song or (staging_songs.Title is null and staging_events.song IS null)))

""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT userId as user_id,
    firstName as first_name, 
    lastName as last_name,
    gender, 
    level
    FROM staging_events
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT song_id, title, artist_id, year, duration
    FROM staging_songs

""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, year, location, latitude, longitude)
    SELECT artist_id, 
    artist_name as name, 
    year, 
    artist_location as location,
    artist_latitude as latitude,
    artist_longitude as longitude
    FROM staging_songs
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT timestamp 'epoch' + ts/1000 * interval '1 second' AS start_time,
    EXTRACT(hour FROM start_time) as hour,
    EXTRACT(day FROM start_time) as day,
    EXTRACT(week FROM start_time) as week,
    EXTRACT(month FROM start_time) as month,
    EXTRACT(year FROM start_time) as year,    
    EXTRACT(weekday FROM start_time) as weekday
    FROM staging_events
""")

# QUERY LISTS


create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
