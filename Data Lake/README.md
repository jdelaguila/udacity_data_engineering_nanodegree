## Project Summary

A fictitious music streaming company has expanded and wants a data engineer to
move their datawarehouse to a data lake. Currently all their data resides in
S3 in JSON format.  As the data engineer, my task is to build an ETL pipeline 
that extracts data stored in Amazon S3 in JSON format, process the data using 
Apache Spark, and then load the data back onto S3 as partitioned parquet files.

## Desired Schema for Data Model

> ### Fact Table
> 
> 1. songplays:  songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

> ### Dimension tables
> 
> 1. users: user_id, first_name, last_name, gender, level
> 2. songs:  song_id, title, artist_id, year, duration
> 3. artists:  artist_id, name, location, lattitude, longitude
> 4. time:  start_time, hour, day, week, month, year, weekday

## Steps taken to complete project

1. Create Spark session.
2. Create a schema for a Spark dataframe to load song data from S3. 
   Create Spark dataframe and load song data from S3 into dataframe.
   This behaves as a staging table for the song data.
3. Using the SQL function of the SparkSession data was extracted from
   the song staging table to create two dataframes: artists, and songs.
   These would serve as the dimension tables of the same name.  These 
   tables are loaded back onto S3 as parquet files.
4. In a similar manner, log data is extracted from S3 and processed to 
   create the users, and time dimension tables. These tables are then 
   loaded back onto S3 as parquet files.
5. Song data and log data are combined to create the songplays table
   which is then loaded back to S3 as a parquet file.

## How to Run Script

To run the etl.py script AWS Access and Secret keys are needed.  Once
those are obtained, place them without quotation marks in the appropriate 
places in the dl.cfg file. Create a new S3 bucket in us-west-2 region and 
enter the S3 URL in etl.py in the main function for the **output_data** variable.
etl.py can be run in the terminal.

## Files

> ##### dl.cfg
> Configuration file for AWS credentials

> ##### test.ipynb
> Jupyter notebook to test etl as script was being built

> ##### etl.py
> Main script to run that extracts data from S3, transforms it into fact and
> dimension tables, and then loads them back on to S3.

> ##### README.md
> Markdown file explaining project.

> ##### .parquet files
> Files loaded to workspace during testing.
