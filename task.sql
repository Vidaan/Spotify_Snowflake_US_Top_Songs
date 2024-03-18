CREATE OR REPLACE STREAM 
spotify_playlist_stream ON TABLE SPOTIFY_DB.US_50_PLAYLIST.SP_TOP_50_US_PLYLST
APPEND_ONLY = TRUE;

-- show details of stream
SHOW STREAMS LIKE '%spotify_playlist_stream%';

-- warehouse to execute the tasks
CREATE WAREHOUSE IF NOT EXISTS task_warehouse WITH WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 120;

-- task to load data from the stream into history table
CREATE OR REPLACE TASK load_data_to_history_table
    WAREHOUSE = task_warehouse
    SCHEDULE = 'USING CRON 0 17 * * 5 America/Chicago'
    WHEN SYSTEM$STREAM_HAS_DATA('spotify_playlist_stream')
AS
    INSERT INTO SPOTIFY_DB.US_50_PLAYLIST.HIST_SP_TOP_50_US_PLYLST
    SELECT * FROM SPOTIFY_DB.US_50_PLAYLIST.SP_TOP_50_US_PLYLST
;

-- task to truncate source table
CREATE OR REPLACE TASK truncate_source_table
    WAREHOUSE = task_warehouse
    AFTER load_data_to_history_table
AS
    TRUNCATE TABLE IF EXISTS SPOTIFY_DB.US_50_PLAYLIST.SP_TOP_50_US_PLYLST
;

-- execute tasks
ALTER TASK load_data_to_history_table RESUME;
ALTER TASK truncate_base_table RESUME;

-- command to manually trigger task
EXECUTE TASK load_data_to_history_table;

-- task history
SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY());