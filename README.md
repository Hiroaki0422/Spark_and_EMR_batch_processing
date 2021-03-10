## About This Project
Date:  Nov 28, 2020  
Author: Hiroaki Oshima  <br>

## Batch Processing Log Files with Spark

This Project demonstrates the process of batch processing log data of a music application (stored as JSON)  in an Object Store AWS S3, which are used as data lake to store raw/unprocessed data. This project is part of the Udacity Data Engineering Nano Degree. 

In this project, I did following:
1. Extract JSON Data from S3 raw data bucket (data lake)
2.  Clean and Transform data using Spark / Spark SQL
3.  Store the processed data in a processed S3 bucket as parquet file

Technology Used: Spark, Spark SQL, AWS S3, IAM

**Context**
"A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results."

## Dataset 
**Raw Data**
Below is the address of the unprocessed data. The data size is about a dozen gigabytes
-   Song data:  `s3://udacity-dend/song_data`
-   Log data:  `s3://udacity-dend/log_data`

The raw data has below fields <br>

Song data: 
*num_songs, artist_id, artist_latitude, artist_longitude, artist_name, song_id, title, duration, year* <br>

Log data:
*artist, auth, firstName, gender, itemInSession, lastName, length, level, location, method, page, registration, sessionId, song, status, ts, userAgent, userId*

**Processed Data**
The data will be used in data warehouse for analytical purpose. Therefore I modeled data in star schema with 3NF dimension tables and fact table. The processed data is stored as apache parquet file which is a columnar storage making loading to data warehouse much faster than row storage, 

#### Fact Table

1.  **songplays**  - records in log data associated with song plays i.e. records with page  `NextSong`
    -   _songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent_

#### Dimension Tables

2.  **users**  - users in the app
    -   _user_id, first_name, last_name, gender, level_
3.  **songs**  - songs in music database
    -   _song_id, title, artist_id, year, duration_
4.  **artists**  - artists in music database
    -   _artist_id, name, location, lattitude, longitude_
5.  **time**  - timestamps of records in  **songplays**  broken down into specific units
    -   _start_time, hour, day, week, month, year, weekday_

## Data Processing Steps
**Processing Song Data**
1. the application will load the raw song data stored in S3 bucket with s3a accessor(faster to load than native s3)
2.  The application extract columns for song tables from the raw data and then remove duplicates and rows that are missing primary column for the table
3. Spark stores the songs table data in processed S3 bucket as parquet file partition by year and artist id
4. Repeat same process above to extract, transform and store artists table while renaming columns

**Processing Log Data**
1.  the application will load the raw log data stored in S3 bucket with s3a accessor
2.  The application filter log data with rows where users took actions to play a song, removing unnecessary rows
3.  Next, the application extract columns for users table removing duplicate and rows with missing user_id which is the primary column 
4. Application writes and stores the users table in the processed bucket
5. The application constructs time table from log data, extract year, month, day, weekday and so on.
6. Time table will be stored in S3 bucket partitioned by year and  month
7.  The application creates temporary views in memory for songs artists tables which we created above when processing the song data
8. Joining the above 2 tables with the log data, the application constructs song_play fact table
9. song_play table will be given a primary index and stored as parquet file partitioned by year and month

# Ruuning the Script  


    python3 etl.py
