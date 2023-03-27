### Purpose:

The goal of this project is to create a Postgres database and ETL pipeline for a ficticious music streaming company Sparkify. As the hired data engineer,
I am tasked with defining fact and dimension tables for a star schema to be used in songplay and user analytics.  I am to create a data pipeline that 
transfers data from files in two local directories into the created Postgres database using python and SQL.  The purpose of this database is to organize 
the data from Sparkify's JSON files so that it can be queried easily to learn more about what songs their users are listening to.

#### To Run Python Scripts:

First create_tables.py must be run to drop any existing tables and recreate them.
Then run etl.py to populate the tables from the log and data files.

### Files:

data - JSON files storing the song data and log data. 
create_tables.py - Drops and creates tables. This file is to be run before running ETL scripts.
etl.ipynb - Reads and processes singleton log and data files into each table. This file was used to develop the etl.py file for loading all data into the tables.
sql_queries.py - Contains all sql statements to be used including dropping tables, creating tables, inserting into tables, and selecting songs.
test.ipynb - Explores database tables and checks for proper datatypes and constraints.

### Database Schema and ETL Pipline:

A Star Schema design is used to organize the data for Sparkify because this allows for quick aggregations.  Using this schema, 
Sparkify can execute simple queries to gather insights about their users such as most active users, total number of free and 
paid users, and most busiest days for listening.

#### Schema
  > ##### Fact table
  > songplays: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

  > ##### Dimension tables
  > 1. users: user_id, first_name, last_name, gender, level
  > 2. songs: song_id, title, artist_id, year, duration
  > 3. artists: artist_id, name, location, latitude, longitude
  > 4. time: start_time, hour, day, week, month, year, weekday

> ETL
> 1. Create songs and artists dimension tables by taking selected columns from songs data.
> 2. Create users and time dimension tables by taking select columns from log data.
> 3. Create songplays fact table with data from previously created tables and log data.

### Example Queries:

##### Most active users

> %sql SELECT u.user_id, u.first_name, u.last_name, COUNT(s.user_id) FROM songplays s JOIN users u ON u.user_id = s.user_id GROUP BY 1, 2, 3 ORDER BY 4 DESC LIMIT 5;

##### Total number of free and paid users

> %sql SELECT level, count(*) FROM songplays GROUP BY 1 ORDER BY 2 DESC;

##### Busiest days for listening

> %sql SELECT date(start_time), count(*) FROM songplays GROUP BY 1 ORDER BY 2 DESC;
