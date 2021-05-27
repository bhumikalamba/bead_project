from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType
from pyspark.sql.functions import col, date_trunc, to_timestamp, to_utc_timestamp, date_format,from_utc_timestamp

import google.auth
import google.cloud
from google.cloud import bigquery

## BQ CONFIG
GOOGLE_APPLICATION_CREDENTIALS = './direct-analog-308416-f082eab9c7fa.json'
cred = google.auth.load_credentials_from_file(GOOGLE_APPLICATION_CREDENTIALS)
PROJECT_ID = 'direct-analog-308416'
client = bigquery.Client(credentials= cred[0],project=cred[1])

appName = "Batch Process to connect GCS, cleanse and store in BQ"
master = "local"

# Create Spark session
spark = SparkSession.builder \
    .appName(appName) \
    .master(master) \
    .getOrCreate()

# Setup hadoop fs configuration for schema gs://
conf = spark.sparkContext._jsc.hadoopConfiguration()
conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
spark.conf.set("google.cloud.auth.service.account.enable", "true")
spark.conf.set("google.cloud.auth.service.account.email", "surendhar@direct-analog-308416.iam.gserviceaccount.com")
spark.conf.set("google.cloud.auth.service.account.keyfile", "./direct-analog-308416-5ab7eaa25c80.p12")
conf.set('spark.jars.packages', 'com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.15.1-beta,com.google.cloud.bigdataoss:gcs-connector:hadoop3-1.9.18')
conf.set('spark.hadoop.fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')
spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")

# Create data frame
gs_file_path = 'gs://tweets-bucket-32'

#df=spark.read.json("gs://tweets-bucket-32/*")
#df = spark.read.json("gs://tweets-bucket-32/FlumeData.1621789271477")
df = spark.read.json("gs://tweets-bucket-demo/FlumeData.1622027472564")
df.printSchema()

# df.select('created_at',(to_timestamp("created_at","EEE MMM d H:mm:ss +0000 y")).alias('tweet_datetime_timestamp')).show(10,truncate=False)

## Remove Retweets and keep only the required columns
df2 = df.where(col("retweeted_status").isNull()).\
    selectExpr("id_str AS tweet_id","user.name AS twitter_handle_name","user.id_str AS twitter_handle_id",
               "created_at AS tweet_datetime","user.description AS twitter_handle_desc","user.followers_count AS followers_count",
               "user.friends_count AS friends_count","user.listed_count AS listed_count","user.created_at AS user_created_at",
               "user.statuses_count AS statuses_count","text AS tweet","truncated")

## Convert datetime string to timestamp. truncate tweet_datetime to hour for analysis purposes
final_df = df2.select("*",to_timestamp("tweet_datetime","EEE MMM d H:mm:ss +0000 y").alias('tweet_datetime_timestamp'),
           to_timestamp("user_created_at","EEE MMM d H:mm:ss +0000 y").alias('created_at_timestamp'))\
    .withColumn("twitter_datetime_hour",date_trunc("hour",col("tweet_datetime_timestamp"))).drop('tweet_datetime','user_created_at')\
    .withColumn("listed_count",df2['listed_count'].cast(StringType())).withColumn("statuses_count",df2['statuses_count'].cast(StringType()))\
    .withColumn("followers_count",df2['followers_count'].cast(StringType())).withColumn("friends_count",df2['friends_count'].cast(StringType()))

bq_SaveMode = 'append'

#set up GCS bucket required by BQ Connector for exporting data
bucket = "temp_spark_bucket_for_flume_data"
spark.conf.set('temporaryGcsBucket', bucket)

bq_path = 'project_data.twitter_data_flume'

final_df.write \
    .format('bigquery') \
    .mode(bq_SaveMode) \
    .option('table', bq_path) \
    .save()







