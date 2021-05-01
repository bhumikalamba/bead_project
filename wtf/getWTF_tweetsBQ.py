# spark job submit
# spark-submit --jars gs://spark-lib/bigquery/spark-bigquery_2.12-0.10.0-beta-shaded.jar --driver-memory 5g --executor-memory 5g getWTF_tweetsBQ.py


import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import *

from datetime import datetime, timedelta

import google.auth
import google.cloud
from google.cloud import bigquery
from google.cloud import storage

############ YOUR CONFIG ############

# Google Credentials setup
GOOGLE_APPLICATION_CREDENTIALS = '/home/gwee_dx/direct-analog-308416-7a6240d97a7c.json'
cred = google.auth.load_credentials_from_file(GOOGLE_APPLICATION_CREDENTIALS)
PROJECT_ID = 'direct-analog-308416'
client = bigquery.Client(credentials= cred[0],project=cred[1])

# File paths to data store (if using local machine for testing only)
#my_tweets_source_file = '/Users/rais/Desktop/post grad/EBAC/2020/CA/canary_v1/bead_project/wtf/sandbox_data/bq-results-20210411-152934-y7k6qt7cfuby.csv'
my_news_source_file = '/Users/rais/Desktop/post grad/EBAC/2020/CA/canary_v1/bead_project/wtf/sandbox_data/dl_files/*'
output_path = '/Users/rais/Desktop/post grad/EBAC/2020/CA/canary_v1/bead_project/wtf/sandbox_data/output_files'

# filter tweets and news data to sample time period
sample_periods = ('2021-03-30 12:00:00','2021-04-03 07:00:00')

# Events (a set of time points in the
# bitfinex price chart that we have
# identified as the start of a significant
# upward or downward shock in price
# note: currently, code only works with exactly 4 events. Have to tweak code if have more/less nbr of events.
#events = ('2021-03-31 06:00:00','2021-03-31 07:00:00','2021-03-31 10:30:00','2021-04-02 01:00:00')
events = ['2021-03-31 06:00:00','2021-03-31 07:00:00','2021-03-31 10:30:00','2021-04-02 01:00:00']

# Bandwidth (the time bandwidth in minutes
# around each event that we want to specify
# as the golden window)
#bw = -15
#bw = '15'
bw = 30
# Save Mode for writing to BQ - overwrite or append
bq_SaveMode = 'append'

# Hard to automatically generate the golden windows.
gw_end = []
gw_start = []
for event in events:
    event = datetime.strptime(event, '%Y-%m-%d %H:%M:%S')
    gw_end.append(event)
    gw_start.append(event - timedelta(minutes=bw))
print(gw_end)
print(gw_start)

gw = {}
for x in range(0,4):
    gw[f'gw{x+1}'] = (gw_start[x], gw_end[x])


'''
# Let's manually specify here instead, based on events and bw.
# note: golden windows can't overlap! otherwise code will mess up
gw1 = ('2021-03-31 05:45:00','2021-03-31 06:00:00')
gw2 = ('2021-03-31 06:45:00','2021-03-31 07:00:00')
gw3 = ('2021-03-31 10:15:00','2021-03-31 10:30:00')
gw4 = ('2021-04-02 00:45:00','2021-04-02 01:00:00')
'''
############ END YOUR CONFIG #########

#:::::::::::::::::::::::::::::::::::::
# > Initialize spark session
#:::::::::::::::::::::::::::::::::::::
spark = SparkSession \
    .builder \
    .master('yarn') \
    .appName('wtf') \
    .config('spark.jars.packages', 'com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.15.1-beta,com.google.cloud.bigdataoss:gcs-connector:hadoop3-1.9.18') \
    .config('spark.hadoop.fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem') \
    .getOrCreate()

#set up GCS bucket required by BQ Connector for exporting data
bucket = "temp_spark_bucket_1"
spark.conf.set('temporaryGcsBucket', bucket)

'''
# Use the Cloud Storage bucket for temporary BigQuery export data used by the connector
bucket = "temp_spark_bucket_1"
spark.conf.set('temporaryGcsBucket', bucket)
'''

#:::::::::::::::::::::::::::::::::::::
# > Build functions
#:::::::::::::::::::::::::::::::::::::

# function to get number of rows and columns of a spark DataFrame
def spark_shape(self):
    return (self.count(), len(self.columns))
pyspark.sql.dataframe.DataFrame.shape = spark_shape
'''
# function to get publications in sample period, and generate some columns related to golden windows
def get_sample(spark_df,publish_datetime):
    return(spark_df \
           .where(f.col(publish_datetime).between(*sample_periods)) \
           .withColumn('publish_in_golden_window',
                       f.when(f.col(publish_datetime).between(*gw1) |
                              f.col(publish_datetime).between(*gw2) |
                              f.col(publish_datetime).between(*gw3) |
                              f.col(publish_datetime).between(*gw4),1).otherwise(0)) \
           .withColumn('golden_window',
                       f.when(f.col(publish_datetime).between(*gw1),1)
                       .when(f.col(publish_datetime).between(*gw2),2)
                       .when(f.col(publish_datetime).between(*gw3),3)
                       .when(f.col(publish_datetime).between(*gw4),4).otherwise(0)))
pyspark.sql.dataframe.DataFrame.wtf = get_sample
'''

# TRY WITH DICTIONARY gw
# function to get publications in sample period, and generate some columns related to golden windows
def get_sample(spark_df,publish_datetime):
    return(spark_df \
           .where(f.col(publish_datetime).between(*sample_periods)) \
           .withColumn('publish_in_golden_window',
                       f.when(f.col(publish_datetime).between(*gw['gw1']) |
                              f.col(publish_datetime).between(*gw['gw2']) |
                              f.col(publish_datetime).between(*gw['gw3']) |
                              f.col(publish_datetime).between(*gw['gw4']), 1).otherwise(0)) \
           .withColumn('golden_window',
                       f.when(f.col(publish_datetime).between(*gw['gw1']), 1)
                       .when(f.col(publish_datetime).between(*gw['gw2']), 2)
                       .when(f.col(publish_datetime).between(*gw['gw3']), 3)
                       .when(f.col(publish_datetime).between(*gw['gw4']), 4).otherwise(0)))
pyspark.sql.dataframe.DataFrame.wtf = get_sample

# function to generate DataFrame for total number of publications for each agent
def get_total_nbr_publish(spark_df,agent):
    return (spark_df \
            .groupBy(agent) \
            .count() \
            .withColumnRenamed('count','total_nbr_publish'))
pyspark.sql.dataframe.DataFrame.wtf = get_total_nbr_publish

# function to generate DataFrame for total number of publications in golden windows for each agent
def get_total_nbr_publish_in_gw(spark_df,agent):
    return (spark_df \
            .filter(spark_df['publish_in_golden_window']==1) \
            .groupBy(agent) \
            .count() \
            .withColumnRenamed('count','total_nbr_publish_in_golden_window'))
pyspark.sql.dataframe.DataFrame.wtf = get_total_nbr_publish_in_gw

# function to generate DataFrame for total number of golden window hits for each agent
def get_nbr_golden_window_hit(spark_df,agent):
    return (spark_df \
            .select(agent,'golden_window') \
            .filter(f.col('golden_window')!=0) \
            .dropDuplicates() \
            .groupBy(agent) \
            .count() \
            .withColumnRenamed('count','nbr_golden_window_hit'))
pyspark.sql.dataframe.DataFrame.wtf = get_nbr_golden_window_hit

# function to generate WTF index for each agent
def get_wtf(spark_df,agent):
    return(get_total_nbr_publish(spark_df,agent) \
           .join(get_total_nbr_publish_in_gw(spark_df,agent),on=agent,how='left') \
           .join(get_nbr_golden_window_hit(spark_df,agent),on=agent,how='left') \
           .fillna({'total_nbr_publish_in_golden_window':'0','nbr_golden_window_hit':'0'}) \
           .withColumn('wtf',(f.col('total_nbr_publish_in_golden_window')/f.col('total_nbr_publish'))*(f.col('nbr_golden_window_hit')/len(events))) \
           .sort(f.col('wtf').desc())
           .select(agent,'total_nbr_publish','total_nbr_publish_in_golden_window','nbr_golden_window_hit','wtf'))




#:::::::::::::::::::::::::::::::::::::
# > Calculate WTF for tweets data
#:::::::::::::::::::::::::::::::::::::
'''
# read in Twitter tweets from csv from local machine

tweets_schema = StructType([
    StructField('tweet_id',StringType(),True),
    StructField('twitter_handle_name',StringType(),True),
    StructField('twitter_handle_id',StringType(),True),
    StructField('tweet_datetime',TimestampType(),True),
    StructField('twitter_handle_desc',StringType(),True),
    StructField('followers_count',IntegerType(),True),
    StructField('friends_count',IntegerType(),True),
    StructField('listed_count',IntegerType(),True),
    StructField('created_at_datetime',TimestampType(),True),
    StructField('twitter_datetime_hour',TimestampType(),True),
    StructField('statuses_count',IntegerType(),True),
    StructField('tweet',StringType(),True),
    StructField('truncated',BooleanType(),True)
])


tweets = spark.read \
    .format('csv') \
    .option('header','true') \
    .option('mode','DROPMALFORMED') \
    .schema(tweets_schema) \
    .load(my_tweets_source_file) \
    .select('tweet_id','twitter_handle_name','twitter_handle_id','tweet_datetime') \
    .dropDuplicates(['tweet_id'])
'''

# read in Twitter tweets from BigQuery
# load from BQ using SQL
spark.conf.set("viewsEnabled","true")
spark.conf.set("materializationDataset","<dataset>")

#use SQL statement to transfer only the results
sql = """
SELECT tweet_id, twitter_handle_name, twitter_handle_id, tweet_datetime
FROM 'direct-analog-308416.project_data.twitter_data_ingest' a
WHERE tweet_datetime >= '2021-03-30' AND tweet_datetime <= '2021-04-03'
LIMIT 10 
"""
tweets = spark.read\
    .format('bigquery') \
    .option('table', 'direct-analog-308416.project_data.twitter_data_ingest') \
    .option('credentialsFile', GOOGLE_APPLICATION_CREDENTIALS) \
    .load(sql)

tweets.show()

tweets = tweets \
    .select('tweet_id','twitter_handle_name','twitter_handle_id','tweet_datetime') \
    .dropDuplicates(['tweet_id'])

twitter_ids = tweets.select('twitter_handle_id','twitter_handle_name').dropDuplicates(['twitter_handle_id'])

tweets = tweets.select('twitter_handle_id','tweet_datetime')

twitter_ids.show()
tweets.show()
tweets.printSchema()


# get tweets in sample period, and generate some columns related to golden windows
tweets_sample = get_sample(tweets,'tweet_datetime')
tweets_sample.filter(f.col('publish_in_golden_window')==1).show()

# generate wtf for agents in twitter
wtf_tweets = get_wtf(tweets_sample,'twitter_handle_id')
wtf_tweets = wtf_tweets \
    .join(twitter_ids,on='twitter_handle_id',how='left') \
    .select('twitter_handle_name','twitter_handle_id','total_nbr_publish','total_nbr_publish_in_golden_window','nbr_golden_window_hit','wtf') \
    .sort(f.col('wtf').desc())

wtf_tweets = wtf_tweets.withColumn('bandwidth_before_event_mins', f.lit(bw))

wtf_tweets.show()


# Saving the data to BigQuery
#filename = 'project_data.wtf_tweets_'+bw
filename = 'project_data.wtf_tweets_15_30'


# wtf_tweets.write.format('bigquery') \
#     .option('table', filename) \
#     .save()

wtf_tweets.write \
    .format('bigquery') \
    .mode(bq_SaveMode) \
    .option('table', filename) \
    .save()

'''
# Save data to local machine
filename = output_path+'/wtf_tweets_'+bw+'.csv'
wtf_tweets.toPandas().to_csv(filename,index=False)
'''

print('Done writing wtf_tweets to '+filename+'.\n\n')
