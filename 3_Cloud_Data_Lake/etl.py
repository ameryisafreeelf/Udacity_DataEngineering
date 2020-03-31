import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


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
    """ 
    Creates song and artist tables in Spark, saves these tables as Parquet
    In: SparkSession object, Path to JSON files stored in S3, Output path for Parquet files 
    """
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*"

    # read song data file
    df = spark.read.json(song_data)
    
    # create a temp view so that we can create a table by SQL queries 
    df.createOrReplaceTempView("songs_temp")
    
    # extract columns to create songs table
    song_table = spark.sql("""
                            SELECT song_id, 
                                title,
                                artist_id,
                                year,
                                duration
                            FROM songs_temp
                            WHERE song_id IS NOT NULL
                        """)
    
    # write songs table to parquet files partitioned by year and artist
    song_table.write.mode('overwrite').partitionBy("year", "artist").parquet(output_data + "song.parquet")
     
    # extract columns to create artists table
    artist_table = spark.sql("""
                    SELECT DISTINCT artist_id, 
                            artist_name,
                            artist_location,
                            artist_latitude,
                            artist_longitude
                    FROM songs_temp
                    WHERE artist_id IS NOT NULL
                    """)
    
    # write artists table to parquet files
    artist_table.write.parquet(output_data + "artist.parquet", mode="overwrite")

def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + "log_data"
    
    # read log data file
    df = spark.read.text(log_data) 
    
    # create a temp view so that we can create a table by SQL queries     
    df.createOrReplaceTempView("log_temp")
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    user_table = spark.sql("""
                    SELECT DISTINCT userId as user_id, 
                            firstName as first_name,
                            lastName as last_name,
                            gender,
                            level
                        FROM log_temp
                        WHERE userId IS NOT NULL
                        """)
    
    # write users table to parquet files
    user_table.write.parquet(output_data + "user.parquet", mode="overwrite")
        
    # extract columns to create time table 
    # A few ways to do this- I will create a subquery and extract from it
    time_table = spark.sql("""
                            SELECT DISTINCT start_time as start_time,
                                hour(CONV.converted_ts) as hour,
                                dayofmonth(CONV.converted_ts) as day,
                                weekofyear(CONV.converted_ts) as week,
                                month(CONV.converted_ts) as month,
                                year(CONV.converted_ts) as year,
                                dayofweek(CONV.converted_ts) as weekday
                            FROM
                                (SELECT to_timestamp(LT.ts/1000) as converted_ts
                                FROM log_temp LT
                                WHERE LT.ts IS NOT NULL
                                ) CONV
                            
                        """)

    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data+'time.parquet/')

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data+'song.parquet/')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
                                SELECT monotonically_increasing_id() as songplay_id,
                                    to_timestamp(LT.ts/1000) as start_time,
                                    month(to_timestamp(LT.ts/1000)) as month,
                                    year(to_timestamp(LT.ts/1000)) as year,
                                    LT.userId as user_id,
                                    LT.level as level,
                                    ST.song_id as song_id,
                                    ST.artist_id as artist_id,
                                    LT.sessionId as session_id,
                                    LT.location as location,
                                    LT.userAgent as user_agent
                                FROM log_data LT
                                JOIN songs_table ST on LT.song = ST.title and LT.artist = ST.artist_name 
                            """)
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data+'songsplays.parquet/')


def main():
    spark = create_spark_session()
    
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://achang-udacity/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
