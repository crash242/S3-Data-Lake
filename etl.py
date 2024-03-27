import configparser
from datetime import datetime
import pandas as pd
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import (year as y, \
                                   month as m, \
                                   dayofmonth as dm, \
                                   hour as h, \
                                   weekofyear as wk, \
                                   dayofweek as dw, \
                                   date_format as dt)

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    print('reading song data...')
    song_data = input_data + "song_data/A/A/A/*.json"
    
    # read song data file
    df = spark.read.json(song_data).dropna(how="any").dropDuplicates()
    #df.na.drop()

    # extract columns to create songs table
    print("extract columns to create songs table")
    songs_table = df.select('song_id', 'artist_id', 'title', 'year', 'duration').distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy('year', 'artist_id').parquet(os.path.join(output_data, 'songs/songs.parquet'))

    # extract columns to create artists table
    print("extract columns to create artists table")
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude').distinct()
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').partitionBy('artist_id').parquet(os.path.join(output_data, 'artists/artists.parquet'))


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*"
    
    # read log data file
    print("reading log data ...")
    log_df = spark.read.json(log_data).dropna(how="any").dropDuplicates()

    # extract columns for users table    
    print("extract columns for users table...")
    users_table = log_df.select('userId', 'firstName', 'lastName', 'gender', 'level').where(col("page") == "NextSong").distinct()
    
    # write users table to parquet files
    users_table.write.mode('overwrite').partitionBy('userId').parquet(os.path.join(output_data, 'users/users.parquet'))

    # create timestamp column from original timestamp column
    print("create timestamp column from original timestamp column...")
    get_timestamp = udf(lambda x: str(int(int(x)/1000)))
    log_df = log_df.withColumn('timestamp', get_timestamp('ts'))
    
    # create datetime column from original timestamp column
    print("create datetime column from original timestamp column...")
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x))))
    log_df = log_df.withColumn('datetime', get_datetime('timestamp'))
    
    # extract columns to create time table
    print("extract columns to create time table...")
    time_table = log_df.select('datetime') \
                    .withColumn('start_time', log_df.datetime) \
                    .withColumn('hour', h('datetime')) \
                    .withColumn('day', dm('datetime')) \
                    .withColumn('week', wk('datetime')) \
                    .withColumn('month', m('datetime')) \
                    .withColumn('year', y('datetime')) \
                    .withColumn('weekday', dw('datetime')) \
                    .dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy('year','month').parquet(os.path.join(output_data, 'time/time.parquet'))    

    # read in song data to use for songplays table
    #song_data = input_data + "song_data/A/A/A/*.json"
    song_df = spark.read.json(input_data + "song_data/A/A/A/*.json").dropna(how="any")
    song_df = song_df.select('artist_id','artist_name','song_id', 'title').dropDuplicates()
    
    print(log_df)
    print(song_df)
    print("joining dataframes...")
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = log_df.join(song_df, (log_df.artist == song_df.artist_name) & \
                     (log_df.song == song_df.title))
    songplays_table = songplays_table.select('songplay_id', 'start_time', 'user_id', 'level', 'song_id', 'artist_id', 'session_id', 'location', 'user_agent')

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy('songplay_id').parquet(os.path.join(output_data, 'songplays/songplays.parquet'))
    print("done")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify-datalake-242/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)

if __name__ == "__main__":
    main()

