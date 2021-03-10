import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType, IntegerType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['KEYS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['KEYS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_subdir_path = "song_data/*/*/*/*.json" 
    song_data = os.path.join(input_data, song_subdir_path)
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(['song_id','title','artist_id','year','duration']).drop_duplicates(subset=['song_id']).dropna(subset=['song_id'])
    print('song table rows count:', songs_table.count())
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy(['year', 'artist_id']).parquet(os.path.join(output_data, 'song'), 'overwrite')

    # extract columns to create artists table
    artists_table = df.select(col('artist_id'),
                              col('artist_name').alias('name'),
                              col('artist_location').alias('location'),
                              col('artist_latitude').alias('latitude'),
                              col('artist_longitude').alias('longitude'))
    print('artist table rows count:', artists_table.count())
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artist'), 'overwrite')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data/*/*/*.json')

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(col('page') == 'NextSong')

    # extract columns for users table, columns user_id, first_name, last_name, gender, level    
    users_table = df.select(['userId', 'firstName', 'lastName', 'gender', 'level']).drop_duplicates(subset=['userID']).dropna(subset=['userID'])
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'user'), 'overwrite')
    print('users table count:', users_table.count())

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: x/1000, IntegerType())
    df = df.withColumn('start_time', get_timestamp('ts'))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x/1000), TimestampType())
    df = df.withColumn('datetime', get_datetime('ts'))
    
    # extract columns to create time table, columns = start_time, hour, day, week, month, year, weekday
    time_table = df.select(col('datetime').alias('start_time'),
                           hour('datetime').alias('hour'),
                           dayofmonth('datetime').alias('day'),
                           weekofyear('datetime').alias('week'),
                           month('datetime').alias('month'),
                           year('datetime').alias('year'),
                           date_format('datetime', 'EEEE').alias('weekday')
                          )
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy(['year','month']).parquet(os.path.join(output_data,'time'),'overwrite')
    print('time table row counts:', time_table.count())

    # read in song and artist data to use for songplays table
    song_df = spark.read.parquet(os.path.join(output_data, 'song'))
    artist_df = spark.read.parquet(os.path.join(output_data, 'artist'))

    # create temp view before join and extracted 
    song_df.createOrReplaceTempView('song')
    df.createOrReplaceTempView('log')
    artist_df.createOrReplaceTempView('artist')
    
    # extract columns from joined song and log datasets to create songplays table 
    columns = songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
    songplays_table = spark.sql("""
                                SELECT l.datetime as start_time, l.userId as user_id, l.level, s.song_id, a.artist_id,
                                        l.sessionId as session_id, l.location, l.userAgent as user_agent, 
                                        year(l.datetime) as year, month(l.datetime) as month
                                FROM log as l 
                                LEFT JOIN song as s ON (l.song = s.title)
                                LEFT JOIN artist as a ON (l.artist = a.name) AND (s.artist_id = a.artist_id)
                                """)
    
    # add a primary key column
    from pyspark.sql.functions import monotonically_increasing_id
    songplays_table = songplays_table.withColumn('songplays_id', monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy(['year', 'month']).parquet(os.path.join(output_data, 'songplay'), 'overwrite')
    print('song play row counts:', songplays_table.count())


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://hiro-datalake/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
