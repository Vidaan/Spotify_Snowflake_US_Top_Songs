-- Create database and schema
CREATE DATABASE IF NOT EXISTS SPOTIFY_DB;
USE DATABASE SPOTIFY_DB;

CREATE SCHEMA IF NOT EXISTS US_50_PLAYLIST;
USE SCHEMA US_50_PLAYLIST;

/* 
Creating the table to store playlist data
Storing JSON data from the file as a VARIANT data type
*/
CREATE TABLE IF NOT EXISTS SPOTIFY_US_50_PLYLST
    (
        TRACK VARIANT,
        ROW_ID VARCHAR,
        LOAD_DATE TIMESTAMP_NTZ
    );

-- Creating the file format
CREATE FILE FORMAT IF NOT EXISTS my_json_format
TYPE = JSON
STRIP_OUTER_ARRAY = TRUE;

-- Creating storage integration to S3
CREATE STORAGE INTEGRATION s3_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<aws_account_id>:role/<snowflake_access_role>'
  STORAGE_ALLOWED_LOCATIONS = ('s3://spotify-playlist-api-bkt-vi//tmp/');

DESC INTEGRATION s3_integration;

-- Granting permission for the snowflake role to create/use the storage integration
GRANT CREATE STAGE ON SCHEMA US_50_PLAYLIST TO ROLE ACCOUNTADMIN;
GRANT USAGE ON INTEGRATION s3_integration TO ROLE ACCOUNTADMIN;

-- Creating external stage
CREATE STAGE SPOTIFY_US_50_PLYLST
  STORAGE_INTEGRATION = s3_integration
  URL = 's3://spotify-playlist-api-bkt-vi//tmp/'
  FILE_FORMAT = my_json_format;

SHOW STAGES;

-- Reference external stage with "@" in front of the stage name
LIST @SPOTIFY_US_50_PLYLST;

/*
Creating the pipe to auto ingest data from S3

COPY INTO statement pulls data from the stage and loads it into the table
it also adds timestamp and UUID data to the respective columns in the table.
*/
CREATE PIPE SPOTIFY_DB.US_50_PLAYLIST.MUSIC_PIPE auto_ingest=true AS
    COPY INTO SPOTIFY_US_50_PLYLST (TRACK, ROW_ID, LOAD_DATE)
    FROM (SELECT t.$1, UUID_STRING(), CURRENT_TIMESTAMP() FROM @SPOTIFY_US_50_PLYLST t)
    FILE_FORMAT = (FORMAT_NAME = 'my_json_format'); 

SHOW PIPES;
