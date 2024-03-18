import snowflake.connector

conn = snowflake.connector.connect(
    user = '<snowflake_username>',
    password = '<snowflake_password>',
    account = '<snowflake_account>'
)

cur = conn.cursor()

cur.execute("CREATE DATABASE IF NOT EXISTS SPOTIFY_DB")
cur.execute("USE DATABASE SPOTIFY_DB")
cur.execute("CREATE SCHEMA IF NOT EXISTS US_50_PLAYLIST")
cur.execute("USE SCHEMA US_50_PLAYLIST")


cur.execute("""CREATE TABLE IF NOT EXISTS SPOTIFY_US_50_PLYLST
            (
            TRACK VARIANT,
            ROW_ID VARCHAR,
            LOAD_DATE TIMESTAMP_NTZ
            )"""
            )


cur.execute("""CREATE FILE FORMAT IF NOT EXISTS my_json_format
                TYPE = JSON
                STRIP_OUTER_ARRAY = TRUE"""
             )

cur.execute("PUT file://V:\projects\Snowflake_API\spotify_usa_50_*.json @%SPOTIFY_US_50_PLYLST")
cur.execute("""
            COPY INTO SPOTIFY_US_50_PLYLST (TRACK, ROW_ID, LOAD_DATE)
            FROM (SELECT t.$1, UUID_STRING(), CURRENT_TIMESTAMP() FROM @%SPOTIFY_US_50_PLYLST t)
            FILE_FORMAT = (FORMAT_NAME = 'my_json_format')
            """)

conn.close()