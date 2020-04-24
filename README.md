#Project:Data Warehouse with AWS Redshift
## Introduction
Sparkify is a startup who have just launched a new music streaming app. Recently they have grown their user base and song database. They want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

Our task is building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. 

## Dataset

There are two dataset we will use in this project.

- **Song Dataset**: Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. 
- **Log Dataset**: This consists of log files in JSON format and contains activity logs from a music streaming app based on specified configurations.The log files are partitioned by year and month. 

## Schema Design

Using the song and log datasets, we'll create a star schema optimized for queries on song play analysis. This will include the following tables: 

### Fact Table

**1. songplays** - records in log data associated with song plays i.e. records with page NextSong

- songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### Dimension Tables
**2. users** - users in the app

- user_id, first_name, last_name, gender, level

**3. songs** - songs in music database

- song_id, title, artist_id, year, duration

**4. artists** - artists in music database

- artist_id, name, location, latitude, longitude

**5. time** - timestamps of records in songplays broken down into specific units

- start_time, hour, day, week, month, year, weekday

## Project Steps
Below are steps we will follow to complete the project:


- First we create a python file named 'sql_queries.py' to write sql statements.   Queries statements including: creating table statement, copying staging tables from s3 statement, inserting value statement

- Then we run create_tables.py to execute the sql statements to create tables.

- Run etl.py file to insert values into the tables

- Lastly we check the result by running queries in Redshift Query editor.
