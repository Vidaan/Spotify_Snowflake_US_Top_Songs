
-- DDL to create tables

-- source table to hold weekly data
CREATE OR REPLACE TABLE SPOTIFY_DB.US_50_PLAYLIST.SP_TOP_50_US_PLYLST (
    SONG_TITLE VARCHAR,
    ARTISTS VARCHAR,
    TRACK_ID VARCHAR,
    POPULARITY NUMBER,
    SONG_DURATION_MINS TIME,
    TRACK_NUMBER NUMBER,
    RELEASE_DATE DATE,
    LOAD_TIMESTAMP TIMESTAMP_NTZ
);

-- history table to hold historical data
CREATE OR REPLACE TABLE SPOTIFY_DB.US_50_PLAYLIST.HIST_SP_TOP_50_US_PLYLST (
    SONG_TITLE VARCHAR,
    ARTISTS VARCHAR,
    TRACK_ID VARCHAR,
    POPULARITY NUMBER,
    SONG_DURATION_MINS TIME,
    TRACK_NUMBER NUMBER,
    RELEASE_DATE DATE,
    LOAD_TIMESTAMP TIMESTAMP_NTZ
);


-- Transformations
INSERT INTO SPOTIFY_DB.US_50_PLAYLIST.SP_TOP_50_US_PLYLST (
    SONG_TITLE,
    ARTISTS,
    TRACK_ID,
    POPULARITY,
    SONG_DURATION_MINS,
    TRACK_NUMBER,
    RELEASE_DATE,
    LOAD_TIMESTAMP
)

WITH flatten_track as (
    SELECT
        sp.track:"track" as track, 
        f.key as key,
        f.value as value,
        sp.load_date,
        sp.row_id
    FROM SPOTIFY_DB.US_50_PLAYLIST.SPOTIFY_US_50_PLYLST sp,
        LATERAL FLATTEN (INPUT => sp.track:"track") f
        ),

pivot_track as (
    SELECT *
    FROM flatten_track ft
    PIVOT (MAX(ft.value) FOR ft.key in ('album', 'artists', 'disc_number',	
                                        'duration_ms', 'episode', 'explicit', 'external_ids', 'external_urls',	
                                        'href',	'id', 'is_local', 'is_playable', 'name', 'popularity',	
                                        'preview_url', 'track', 'track_number', 'type', 'uri'))
),

list_artist as (

    SELECT 
        pt."'id'"::VARCHAR as track_id,
        pt."'name'"::VARCHAR as song_title,
        f1.value:"name"::VARCHAR as artist_name,
        pt."'popularity'"::NUMBER as popularity,
        TO_TIME(floor(pt."'duration_ms'"/(60*1000)) || ':' || left((pt."'duration_ms'"/(60)),2), 'MI:SS') as song_duration_mins,
        pt."'track_number'"::NUMBER as track_number,
        (pt."'album'":"release_date")::DATE as release_date,
        pt.row_id
        
    FROM pivot_track pt,
    LATERAL FLATTEN (INPUT => pt."'artists'") f1

)

SELECT 
    l.SONG_TITLE, 
    LISTAGG(DISTINCT ARTIST_NAME, ', ') WITHIN GROUP (ORDER BY ARTIST_NAME) AS ARTISTS, 
    l.track_id, 
    l.popularity, 
    l.song_duration_mins, 
    l.track_number, 
    l.release_date, 
    CURRENT_TIMESTAMP()
FROM list_artist l
GROUP BY l.SONG_TITLE, l.track_id, l.popularity, l.song_duration_mins, l.track_number, l.release_date
;


-- List or remove files from staging
LIST @%SPOTIFY_US_50_PLYLST;
REMOVE @%SPOTIFY_US_50_PLYLST;