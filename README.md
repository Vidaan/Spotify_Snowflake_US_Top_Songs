# Data pipeline with Spotify API and Snowflake using Python

![image](https://github.com/Vidaan/Spotify_Snowflake_US_Top_Songs/assets/56769902/b5d6c7cd-c533-46f3-82b0-653d40f95944)

## Objective
Get all the songs listed under Top Songs - USA playlist from Spotify API using Requests library in Python and transform the data and store it in a JSON file.
![image](https://github.com/Vidaan/Spotify_Snowflake_US_Top_Songs/assets/56769902/177963fa-cba6-470d-a173-d7406d040979)

Link to spotify - https://open.spotify.com/playlist/37i9dQZEVXbLp5XoPON0wI

Ingest the file into Snowflake using PUT and COPY INTO commands into a base table that will hold the JSON data in a single column with one row for every track on the playlist.
Then we shall transform the JSON (semi-structured) data by splitting it into columns that are needed using commands/functions like LATERAL, FLATTEN, PIVOT and LISTAGG. After this, insert the data into a new table (SP_TOP_50_US_PLYLST) that will hold the final result which shall look like this…
![image](https://github.com/Vidaan/Spotify_Snowflake_US_Top_Songs/assets/56769902/4ea6b05a-13d6-4c87-82aa-e9dce5a1205d)

After this, as an additional setup, load the data from SP_TOP_50_US_PLYLST table into a history table (HIST_SP_TOP_50_US_PLYLST) that shall contain historical records whereas SP_TOP_50_US_PLYLST shall only contain the current weeks data. This is accomplished by using STREAMS and TASKS.

Refer to my Medium post for more detailed explanation on how to execute it.
https://medium.com/@vidaan95/data-pipeline-with-spotify-api-and-snowflake-using-python-845681ce9e71
