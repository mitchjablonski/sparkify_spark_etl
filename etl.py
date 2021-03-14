import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.types import TimestampType
import posixpath


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    '''
    Create a spark session if none exists, or grab the existing session
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
    Takes in our spark instance, and an input and output data path.
    
    We'll take the columns of interest from both file and write these to our desired tables.
    '''
    # get filepath to song data file
    song_data = posixpath.join(input_data, 'song_data/*/*/*/*.json')
    
    # read song data file
    df = spark.read.json(song_data)
    
    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').filter(col('song_id').isNotNull()).drop_duplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(posixpath.join(output_data, "songs_table/"), mode="overwrite", partitionBy=["year","artist_id"])

    # extract columns to create artists table
    artists_table = df.select('artist_id', col('artist_name').alias('name'), col('artist_location').alias('location'),
                               col('artist_latitude').alias('latitude'), col('artist_longitude').alias('longitude')) \
                      .filter(col('artist_id').isNotNull()).drop_duplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(posixpath.join(output_data, "artists_table/"), mode="overwrite", partitionBy=["artist_id"])


def process_log_data(spark, input_data, output_data):
    '''
    Takes in our spark instance, and an input and output data path.
    
    We'll take the columns of interest from both file and write these to our desired tables.
    '''
    # get filepath to log data file
    log_data =posixpath.join(input_data, 'log_data/*/*/*.json')

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(col('page') == 'NextSong')

    # extract columns for users table    
    users_table = df.select(col("userId").alias('user_id'), col("firstName").alias('first_name'), 
                            col("lastName").alias('last_name'),"gender","level").filter(col('userId').isNotNull()).drop_duplicates()
    
    # write users table to parquet files
    users_table.write.parquet(posixpath.join(output_data, "users_table/"), mode="overwrite")

    # create timestamp column from original timestamp column
    df = df.withColumn('start_time', (df.ts/1000).cast(dataType=TimestampType()))
    
    # extract columns to create time table
    time_table = df_log.withColumn("hour",hour("start_time"))\
                       .withColumn("day",dayofmonth("start_time"))\
                       .withColumn("week",weekofyear("start_time"))\
                       .withColumn("month",month("start_time"))\
                       .withColumn("year",year("start_time"))\
                       .withColumn("weekday",dayofweek("start_time"))\
                       .select("start_time", "hour", "day", "week", "month", "year", "weekday").drop_duplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(posixpath.join(output_data, "time_table/"), mode="overwrite", partitionBy=["year", "month"])

    # read in song data to use for songplays table
    song_df =  spark.read.parquet(posixpath.join(output_data,'songs_table/'))

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, df.song == song_df.title, how='inner')\
                        .select(col("start_time"),col("userId").alias("user_id"),
                                "level","song_id","artist_id", col("sessionId").alias("session_id"), "location", col("userAgent").alias("user_agent")).drop_duplicates()
    songplays_table = songplays_table.withColumn('songplay_id', monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(posixpath.join(output_data, "songplays_table/"), mode="overwrite")


def main():
    '''
    Our main function for kicking off our processing pipeline.
    '''
    spark = create_spark_session()
    input_data = "s3a://udacity-dend"
    output_data = "s3a://sparkify-data-processed"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
