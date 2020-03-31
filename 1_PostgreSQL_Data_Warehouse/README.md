## Problem and Project Goal

Sparkify collects and owns two critical sources of data related to its streaming application- song information, and event log queries about song streams. These data are currently stored in bulk JSON files, so there is currently no convenient and scalable infrastructure for analyzing it. Therefore, Sparkify intends to build a data warehouse such that this data can be analyzed. Specifically, the data warehouse should optimally allow users to query on the song play information from the event logs.

## Data Design

A relational model makes sense here. Our data is currently well-structured already, since it is already stored as JSON. Additionally, the goal of the project is analytics, so we should expect to perform aggregations. A relational design will be more efficient at fetching multiple rows from tables (bag-based query execution) than a NoSQL database (typically tuple-at-a-time query execution), which is more efficient for analytics work. There also does not seem to be a need for horizontal scaling at the moment. A relational data model is clearly preferable for this project.   

For the goal of creating an analytics environment, and the nature of the data (i.e. that the primary thing we will be analyzing is a business event- song streams), a dimensional design (star or snowflake schema) is extremely sensible and intuitive. The grain of the most important fact table will be song streams, so the tuples in the event log will become rows in the fact table. The song information will provide information for dimension tables about the song streams. While the length of the fact table(s) may slow querying down in the future, smart partitioning (e.g. on the date of the song stream) can be helpful for maintaining query performance. 

We will munge the data from the JSON files to PostGreSQL tables using Python scripts. If further data is added to the JSON files in the future and we wish to capture that data, we can add new columns to the schema. If updates to historical rows are ever needed, we can amend historical data by performing upsert scripts to the existing tables. So, our ETL pipeline makes it fairly simple both to add dimension (columns) and volume (rows).

## Sample Query

Find the names and artists of songs that were played at 00:00 UTC on January 1, 2020 in New York City  
`
SELECT DISTINCT songs.title, DISTINCT artists.name
FROM songplays 
FULL OUTER JOIN songs, artists, time
ON songplays.song_id = songs.song_id, songplays.artist_id = artists.artist_id, songplays.start_time = time.start_time
WHERE
        time.hour = 0 AND 
        time.day = 1 AND
        time.month = 1 AND 
        time.year = 2020 AND 
        songplays.location = "New York City"
`