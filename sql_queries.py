# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users, temp_user"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time, temp_time"

# CREATE TABLES

songplay_table_create = ("""
  CREATE TABLE IF NOT EXISTS songplays (
    songplay_id varchar PRIMARY KEY NOT NULL, 
    start_time time NOT NULL, 
    user_id int NOT NULL, 
    level varchar, 
    song_id varchar NOT NULL, 
    artist_id varchar NOT NULL, 
    session_id int, 
    location text, 
    user_agent text
    )
""")

user_table_create = ("""
  CREATE TABLE IF NOT EXISTS users (
    user_id int PRIMARY KEY NOT NULL, 
    first_name varchar, 
    last_name varchar, 
    gender varchar, 
    level varchar
    )
""")

song_table_create = ("""
  CREATE TABLE IF NOT EXISTS songs (
    song_id varchar PRIMARY KEY NOT NULL, 
    title varchar, 
    artist_id varchar, 
    year int, 
    duration float
    )
""")

artist_table_create = ("""
  CREATE TABLE IF NOT EXISTS artists (
    artist_id varchar PRIMARY KEY NOT NULL, 
    name varchar, 
    location varchar, 
    latitude float, 
    longitude float
    )
""")

time_table_create = ("""
  CREATE TABLE IF NOT EXISTS time (
    start_time time PRIMARY KEY NOT NULL, 
    hour int, 
    day int, 
    week int, 
    month int, 
    year int, 
    weekday int
    )
""")

temp_user_table_create = ("""

  CREATE TABLE IF NOT EXIST temp_user (
    LIKE users INCLUDING DEFAULTS
    ) 

""")

temp_time_table_create = ("""

  CREATE TABLE IF NOT EXIST temp_time (
    LIKE time INCLUDING DEFAULTS
    ) 

""")

# INSERT RECORDS

song_table_insert = ("""

  INSERT INTO songs (song_id, title, artist_id, year, duration) VALUES(%s, %s, %s, %s, %s)
  ON CONFLICT DO NOTHING
  
""")

artist_table_insert = ("""

  INSERT INTO artists (artist_id, name, location, latitude, longitude) VALUES(%s, %s, %s, %s, %s)
  ON CONFLICT DO NOTHING
  
""")

user_table_insert = ("""

    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT ON (user_id) *
    FROM temp_user ON CONFLICT (user_id) 
    DO UPDATE SET level=EXCLUDED.level

""")

time_table_insert = ("""

    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT ON (start_time) *
    FROM temp_time ON CONFLICT DO NOTHING

""")


# FIND SONGS

song_select = ("""
  SELECT song_id, songs.artist_id
  FROM songs
  JOIN artists
  ON songs.artist_id = artists.artist_id
  WHERE title = %s
  AND name = %s
  AND duration = %s
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create,temp_user_table_create, temp_time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
