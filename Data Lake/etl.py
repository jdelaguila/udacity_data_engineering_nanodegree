import configparser
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType, TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['KEYS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['KEYS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    This function creates a spark session and returns it to be used in the rest of the etl.py script.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    This function takes the song data from S3, loads them into a dataframe from which the dimension tables for songs
    and artists are created.  These tables are then loaded back on to S3.
    """
    
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    song_schema = StructType([
                        StructField("num_songs", IntegerType()),
                        StructField("artist_id", StringType()),
                        StructField("artist_latitude", DoubleType()),
                        StructField("artist_longitude", DoubleType()),
                        StructField("artist_location", StringType()),
                        StructField("artist_name", StringType()),
                        StructField("song_id", StringType()),
                        StructField("title", StringType()),
                        StructField("duration", DoubleType()),
                        StructField("year", IntegerType())
                        ])
    
    song_df = spark.read.json(song_data, schema=song_schema)
    song_df.createOrReplaceTempView("songs_view")

    # extract columns to create songs table
    songs_table = spark.sql("""
                            SELECT song_id, title, artist_id, year, duration
                            FROM songs_view
                            WHERE song_id IS NOT NULL
                            """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").mode('overwrite').parquet(output_data + "songs_table/songs.parquet")

    # extract columns to create artists table
    artists_table = spark.sql("""
                              SELECT DISTINCT artist_id, artist_name AS name, artist_location AS location, artist_latitude AS latitude, artist_longitude AS longitude
                              FROM songs_view
                              WHERE artist_id IS NOT NULL
                              """)
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + "artists_table/artists.parquet")


def process_log_data(spark, input_data, output_data):
    """
    This function takes the log data from S3, loads them into a dataframe from which the dimension tables for users
    and time are created.  The songplays fact table is also created here. These tables are then loaded back on to S3.
    """
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    log_df = spark.read.json(log_data)
    
    # filter by actions for song plays
    log_df = log_df.filter(log_df.page=='NextSong')
    log_df.createOrReplaceTempView("log_view")

    # extract columns for users table    
    users_table = spark.sql("""
                            SELECT DISTINCT userId AS user_id, firstName AS first_name, lastName AS last_name, gender, level
                            FROM log_view
                            WHERE userId IS NOT NULL
                            """)
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data + "users_table/users.parquet")

    # create timestamp column from original timestamp column
    get_timestamp_no_milliseconds = udf(lambda x: int(int(x)/1000), IntegerType())
    log_df = log_df.withColumn('timestamp_no_milliseconds', get_timestamp_no_milliseconds(log_df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x), TimestampType())
    log_df = log_df.withColumn("start_time", get_datetime(log_df.timestamp_no_milliseconds))
    
    log_df = log_df.withColumn('year', year(log_df.start_time))
    log_df = log_df.withColumn('month', month(log_df.start_time))
    log_df = log_df.withColumn('week', weekofyear(log_df.start_time))
    log_df = log_df.withColumn('weekday', date_format(log_df.start_time, 'E'))
    log_df = log_df.withColumn('day', dayofmonth(log_df.start_time))
    log_df = log_df.withColumn('hour', hour(log_df.start_time))
    
    # extract columns to create time table
    log_df.createOrReplaceTempView("log_view_2")
    
    time_table = spark.sql("""
                           SELECT DISTINCT start_time, hour, day, week, weekday, year, month
                           FROM log_view_2
                           """)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").mode('overwrite').parquet(output_data + 'time_table/time.parquet')

    # read in song data to use for songplays table
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    song_schema = StructType([
                        StructField("num_songs", IntegerType()),
                        StructField("artist_id", StringType()),
                        StructField("artist_latitude", DoubleType()),
                        StructField("artist_longitude", DoubleType()),
                        StructField("artist_location", StringType()),
                        StructField("artist_name", StringType()),
                        StructField("song_id", StringType()),
                        StructField("title", StringType()),
                        StructField("duration", DoubleType()),
                        StructField("year", IntegerType())
                        ])
    
    song_df = spark.read.json(song_data, schema=song_schema)
    song_df.createOrReplaceTempView("songs_view_2")

    # extract columns from joined song and log datasets to create songplays table     
    songplays_table = spark.sql("""
                                SELECT monotonically_increasing_id() AS songplay_id, l.start_time AS start_time, l.userId AS user_id,
                                       l.level AS level, s.song_id AS song_id, s.artist_id AS artist_id, l.sessionId AS session_id,
                                       l.location AS location, l.userAgent AS user_agent, l.year AS year, l.month AS month
                                FROM songs_view_2 s
                                JOIN log_view_2 l ON s.artist_name = l.artist AND s.title = l.song
                                """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").mode('overwrite').parquet(output_data + "songplays_table/songplays.parquet")


def main():
    """
    Main function that runs process_song_data function and process_log_data function.  Note that the output_data variable 
    must be filled in with a created S3 bucket URL.
    """
    spark = create_spark_session()
    input_data = 's3a://udacity-dend/'
    
    # fill in s3 url below
    output_data = ''
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
