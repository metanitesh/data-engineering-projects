import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import DateType
from pyspark.sql.functions import from_unixtime

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['aws']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['aws']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():

    """ Create spark session
        Returns:
            spark{ object }: spark session object
    """
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    
    return spark
    
    

def process_song_data(spark, input_data, output_data):
    """ Process the song files and upload it back to s3

        Arguments:
            spark {object}: Spark object.
            input_data {string}: location of s3 bucket to download data.
            output_data {string}: location of s3 bucket to upload data.
        Returns:
            
    """
    
#   loading song data
    song_data = input_data + 'song-data/A/A/*/*.json'
    df = spark.read.json(song_data)  

#   song table    
    dfsong = df.select(['song_id', 'title', 'artist_id', 'year', 'duration']).dropDuplicates()
    dfsong.write.partitionBy('year', 'artist_id').parquet(output_data+'table/song.parquet')
    
#   artist table
    dfartist = df.select(['artist_id', 'artist_name', 'artist_location', 'artist_longitude', 'duration']).dropDuplicates()
    dfartist.write.parquet(output_data+'table/artist.parquet')


def process_log_data(spark, input_data, output_data):
    """ Process the logs files and upload it back to s3

        Arguments:
            spark {object}: Spark object.
            input_data {string}: location of s3 bucket to download data.
            output_data {string}: location of s3 bucket to upload data.
        Returns:
            
    """
#   loading log data
    log_data = input_data + 'log_data/*/*/*.json';
    df = spark.read.json(log_data)

#   user table
    dfuser = df.select(['userId', 'firstName', 'lastName', 'gender', 'level']).dropDuplicates()
    dfuser_valid = dfuser.filter(dfuser["userId"] != "")

    dfuser_valid.write.parquet(output_data+'table/user.parquet')

#   time table
    isWeekday = udf (lambda x:  True if (int(x) <= 5) else False )
    
    dftime = df.select('ts').dropDuplicates()
    dftime = dftime.withColumn("timestamp", from_unixtime(dftime.ts.cast('bigint')/1000));
    dftime = dftime.withColumn("hour", hour(dftime.timestamp).cast('timestamp'));
    dftime = dftime.withColumn("day", dayofmonth(dftime.timestamp));
    dftime = dftime.withColumn("week", weekofyear(dftime.timestamp));
    dftime = dftime.withColumn("month", month(dftime.timestamp));
    dftime = dftime.withColumn("year", year(dftime.timestamp));
    dftime = dftime.withColumn("dow", date_format(dftime.timestamp, 'u').cast('integer'));
    dftime = dftime.withColumn("weekday", isWeekday(dftime.dow).cast('boolean'));
    
    dftime.write.partitionBy('year', 'month').parquet(output_data+'table/time.parquet')

#   songplay table
    dfsong = spark.read.parquet(output_data+'table/song.parquet')
    
    dfsong.createOrReplaceTempView("songs")
    df.createOrReplaceTempView("logs")

    dfSongplay = spark.sql("""
        SELECT logs.ts as start_time, 
        logs.userId as user_id, 
        logs.level,
        songs.song_id as song_id, 
        songs.artist_id as artist_id, 
        logs.sessionId as session_id,
        logs.location, 
        logs.userAgent as user_agent
        FROM logs LEFT JOIN songs
        ON (songs.title=logs.song)
    """)
    
    def processTimestamp(extractParam):
        return extractParam(from_unixtime(dfSongplay.start_time.cast('bigint')/1000))
        
    dfSongplay \
    .withColumn('year',processTimestamp(year)) \
    .withColumn('month',processTimestamp(month)) \
    .write.partitionBy('year', 'month').parquet(output_data+'table/songplay.parquet')



def main():
    spark = create_spark_session()
  
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://uspdata/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
