import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events 
                                 (
                                      artist VARCHAR,
                                      auth VARCHAR,
                                      firstName VARCHAR,
                                      gender VARCHAR,
                                      itemInSession INT,
                                      lastName VARCHAR,
                                      length NUMERIC,
                                      level VARCHAR,
                                      location VARCHAR,
                                      method VARCHAR,
                                      page VARCHAR,
                                      registration VARCHAR,
                                      sessionId INT,
                                      song VARCHAR,
                                      status INT,
                                      ts TIMESTAMP,
                                      userAgent VARCHAR,
                                      userId INT
                                 );
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs
                                 ( 
                                      num_songs INT,
                                      artist_id VARCHAR,
                                      artist_latitude NUMERIC,
                                      artist_longitude NUMERIC,
                                      artist_location VARCHAR,
                                      artist_name VARCHAR,
                                      song_id VARCHAR,
                                      title VARCHAR,
                                      duration NUMERIC,
                                      year INT
                                  );
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays
                            (
                                songplay_id INT IDENTITY(0,1),
                                start_time TIMESTAMP,
                                user_id INT,
                                level VARCHAR,
                                song_id VARCHAR,
                                artist_id VARCHAR,
                                session_id INT,
                                location VARCHAR,
                                user_agent VARCHAR
                            );
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users
                        (
                            user_id INT,
                            first_name VARCHAR,
                            last_name VARCHAR,
                            gender VARCHAR,
                            level VARCHAR
                        );
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs
                        (
                            song_id VARCHAR,
                            title VARCHAR,
                            artist_id VARCHAR,
                            year INT,
                            duration NUMERIC
                        );
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists 
                          (
                              artist_id VARCHAR,
                              name VARCHAR,
                              location VARCHAR,
                              latitude NUMERIC,
                              longitude NUMERIC
                          );
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time
                        (
                            start_time TIMESTAMP,
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
                      REGION 'us-west-2'
                      JSON {}
                      TIMEFORMAT 'epochmillisecs';
""").format(config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
                      COPY staging_songs FROM {}
                      CREDENTIALS 'aws_iam_role={}'
                      REGION 'us-west-2'
                      JSON 'auto';
""").format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
                        INSERT INTO songplays(start_time, user_id, level, song_id, 
                        artist_id, session_id, location, user_agent)
                        SELECT se.ts AS start_time, 
                               se.userId AS user_id,
                               se.level AS level,
                               ss.song_id AS song_id,
                               ss.artist_id AS artist_id,
                               se.sessionId AS session_id,
                               se.location AS location,
                               se.userAgent AS user_agent
                        FROM staging_events se
                        JOIN staging_songs ss ON (se.artist = ss.artist_name AND se.song = ss.title)
                        WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
                    INSERT INTO users (user_id, first_name, last_name, gender, level)
                    SELECT DISTINCT userId AS user_id,
                           firstName AS first_name,
                           lastName AS last_name,
                           gender AS gender,
                           level AS level
                    FROM staging_events se
                    WHERE se.ts = (SELECT MAX(ts)
                                   FROM staging_events se2
                                   WHERE se2.userId = se.userId)
                    ORDER BY userId DESC;
""")

song_table_insert = ("""
                    INSERT INTO songs (song_id, title, artist_id, year, duration)
                    SELECT song_id,
                           title,
                           artist_id,
                           year,
                           duration
                    FROM staging_songs;
""")

artist_table_insert = ("""
                      INSERT INTO artists (artist_id, name, location, latitude, longitude)
                      SELECT artist_id AS artist_id,
                             artist_name AS name,
                             artist_location AS location,
                             artist_latitude AS latitude,
                             artist_longitude AS longitude
                      FROM staging_songs;
""")

time_table_insert = ("""
                    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
                    SELECT s.start_time AS start_time,
                           EXTRACT(hour FROM s.start_time) AS hour,
                           EXTRACT(day FROM s.start_time) AS day,
                           EXTRACT(week FROM s.start_time) AS week,
                           EXTRACT(month FROM s.start_time) AS month,
                           EXTRACT(year FROM s.start_time) AS year,
                           EXTRACT(dow FROM s.start_time) AS weekday
                    FROM songplays s;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
