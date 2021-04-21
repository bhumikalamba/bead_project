import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import *


############ YOUR CONFIG ############

# File paths to data store (if using local machine for testing only)
my_tweets_source_file = '/Users/rais/Desktop/post grad/EBAC/2020/CA/canary_v1/bead_project/wtf/sandbox_data/bq-results-20210411-152934-y7k6qt7cfuby.csv'
my_news_source_file = '/Users/rais/Desktop/post grad/EBAC/2020/CA/canary_v1/bead_project/wtf/sandbox_data/dl_files/*'
output_path = '/Users/rais/Desktop/post grad/EBAC/2020/CA/canary_v1/bead_project/wtf/sandbox_data/output_files'

# filter tweets and news data to sample time period
sample_periods = ('2021-03-30 12:00:00','2021-04-03 07:00:00')

# Events (a set of time points in the
# bitfinex price chart that we have
# identified as the start of a significant
# upward or downward shock in price
# note: currently, code only works with exactly 4 events. Have to tweak code if have more/less nbr of events.
events = ('2021-03-31 04:00:00','2021-03-31 06:00:00','2021-03-31 12:00:00','2021-04-02 00:00:00')

# Bandwidth (the time bandwidth in minutes
# around each event that we want to specify
# as the golden window)
#bw = -15
bw = '15'

# Hard to automatically generate the golden windows.
# Let's manually specify here instead, based on events and bw.
# note: golden windows can't overlap! otherwise code will mess up
gw1 = ('2021-03-31 03:45:00','2021-03-31 04:00:00')
gw2 = ('2021-03-31 05:45:00','2021-03-31 06:00:00')
gw3 = ('2021-03-31 11:45:00','2021-03-31 12:00:00')
gw4 = ('2021-04-01 23:45:00','2021-04-02 00:00:00')

############ END YOUR CONFIG #########

#:::::::::::::::::::::::::::::::::::::
# > Initialize spark session
#:::::::::::::::::::::::::::::::::::::

spark = SparkSession \
    .builder \
    .appName("wtf") \
    .getOrCreate()

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
tweets = spark.read \
    .format('bigquery') \
    .option('table', 'direct-analog-308416:project_data.tweets_data') \
    .option("filter", "tweet_datetime >= '2021-03-30' AND tweet_datetime <= '2021-04-03'") \
    .load()

tweets.show()

tweets = tweets \
    .select('tweet_id','twitter_handle_name','twitter_handle_id','tweet_datetime') \
    .dropDuplicates(['tweet_id'])
'''

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
wtf_tweets.show()

'''
# Saving the data to BigQuery
filename = 'project_data.wtf_tweets_'+bw
wtf_tweets.write.format('bigquery') \
    .option('table', filename) \
    .save()
'''

# Save data to local machine
filename = output_path+'/wtf_tweets_'+bw+'.csv'
wtf_tweets.toPandas().to_csv(filename,index=False)

print('Done writing wtf_tweets to '+filename+'.\n\n')


#:::::::::::::::::::::::::::::::::::::
# > Calculate WTF for news data
#:::::::::::::::::::::::::::::::::::::

# read in news articles
news = spark.read \
    .format("parquet") \
    .load(my_news_source_file) \
    .withColumnRenamed('date_publish','date_publish_str') \
    .withColumn('date_publish', f.col('date_publish_str').cast(TimestampType())) \
    .select('source_domain','title','description','date_publish') \
    .dropDuplicates(['date_publish', 'title', 'description', 'source_domain']) \
    .select('source_domain','date_publish')

news.show()
news.printSchema()


# get news in sample period, and generate some columns related to golden windows
news_sample = get_sample(news,'date_publish')
news_sample.filter(f.col('publish_in_golden_window')==1).show()

# generate wtf for agents in news data
wtf_news = get_wtf(news_sample,'source_domain')
wtf_news.show()

'''
# Saving the data to BigQuery
filename = 'direct-analog-308416:project_data.wtf_news_'+bw
wtf_news.write.format('bigquery') \
    .option('table', filename) \
    .save()
'''

# Save data to local machine
filename = output_path+'/wtf_news_'+bw+'.csv'
wtf_news.toPandas().to_csv(filename,index=False)

print('Done writing wtf_news_'+bw+' to '+filename+'.\n\n')