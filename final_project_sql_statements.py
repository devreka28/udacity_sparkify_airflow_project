copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON {}
        
    """


class SqlQueries:

    staging_events_table_create = ("""CREATE TABLE staging_events
        ( artist varchar(200),
        auth varchar(50),
        firstName varchar(100),
        gender varchar(20),
        itemInSession bigint,
        lastname varchar(200),
        length numeric(10,4),
        level varchar(10),
        location varchar(300),
        method varchar(10),
        page varchar(20),
        registration bigint,
        sessionId bigint,
        song varchar(200),
        status bigint,
        ts timestamp,
        userAgent varchar(200),
        userId bigint)
        """) 
    
    


    staging_songs_table_create = ("""CREATE TABLE staging_songs
        ( num_songs bigint,
        artist_id varchar(1000),
        artist_latitude numeric(9,6),
        artist_longitude numeric(9,6),
        artist_location varchar(1000),
        artist_name varchar(1000),
        song_id varchar(1000),
        title varchar(1000),
        duration numeric(10,6),
        year bigint)
        """)

    songplay_table_create = ("""CREATE TABLE songplays
    (songplay_id bigint IDENTITY(0,1) PRIMARY KEY,
    start_time timestamp NOT NULL,
    user_id bigint NOT NULL,  
    level varchar(300),
    song_id varchar (300) NOT NULL ,
    artist_id varchar (300) NOT NULL ,
    session_id bigint,
    location varchar(300),
    user_agent varchar(300)
    );
    """)

    songplay_table_insert = ("""
        INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
        SELECT
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT ts AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
            WHERE songs.song_id is not null
    """)
    user_table_create =("""CREATE TABLE users
        (user_id bigint PRIMARY KEY,
        first_name varchar(100),
        last_name varchar(200),
        gender varchar(30),
        level varchar(200)
        );
     """)


    user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_create = ("""CREATE TABLE song
        (song_id varchar(50) NOT NULL PRIMARY KEY,
        title varchar(200), 
        artist_id varchar(50) NOT NULL,
        year bigint, 
        duration numeric(8,4));
    """)

    song_table_insert = ("""INSERT INTO song (song_id, title, artist_id, year, duration)
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_create = ("""CREATE TABLE artist
        (artist_id varchar(100) NOT NULL PRIMARY KEY, 
        name varchar(1000),
        location varchar(400), 
        latitude numeric(9,6), 
        longitude numeric(9,6));
    """)

    artist_table_insert = ("""INSERT INTO artist (artist_id, name, location, latitude, longitude)
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_create = ("""CREATE TABLE time(
        start_time timestamp NOT NULL PRIMARY KEY, 
        hour bigint NOT NULL, 
        day bigint NOT NULL,
        week bigint NOT NULL,
        month bigint NOT NULL,
        year bigint NOT NULL,
        weekday varchar(20) NOT NULL
        );
    """)

    time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)
