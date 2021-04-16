import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import *


############ YOUR CONFIG ############

# File paths to data store
my_tweets_source_file = '/Users/rais/Desktop/post grad/EBAC/2020/CA/canary_v1/bead_project/analysis/sandbox_data/bq-results-20210411-152934-y7k6qt7cfuby.csv'
my_news_source_file = '/Users/rais/Desktop/post grad/EBAC/2020/CA/canary_v1/bead_project/analysis/sandbox_data/dl_files/*'

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
bw = -10

# Hard to automatically generate the golden windows.
# Let's manually specify here instead, based on events and bw.
# note: golden windows can't overlap! otherwise code will mess up
gw1 = ('2021-03-31 03:50:00','2021-03-31 04:00:00')
gw2 = ('2021-03-31 05:50:00','2021-03-31 06:00:00')
gw3 = ('2021-03-31 11:50:00','2021-03-31 12:00:00')
gw4 = ('2021-04-01 23:50:00','2021-04-02 00:00:00')

############ END YOUR CONFIG #########

#:::::::::::::::::::::::::::::::::::::
# > Initialize spark session
#:::::::::::::::::::::::::::::::::::::

spark = SparkSession \
    .builder \
    .appName("wtf") \
    .getOrCreate()

# function to get number of rows and columns of a spark DataFrame
def spark_shape(self):
    return (self.count(), len(self.columns))
pyspark.sql.dataframe.DataFrame.shape = spark_shape


#:::::::::::::::::::::::::::::::::::::
# > Calculate WTF for tweets data
#:::::::::::::::::::::::::::::::::::::

# read in Twitter tweets
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
    .load(my_tweets_source_file)

tweets.show()
tweets.printSchema()

# get tweets in sample period, and generate some columns related to golden windows
tweets_sample = tweets \
    .select('tweet_id','twitter_handle_name','twitter_handle_id','tweet_datetime') \
    .where(f.col('tweet_datetime').between(*sample_periods)) \
    .dropDuplicates(['tweet_id']) \
    .select('twitter_handle_name','twitter_handle_id','tweet_datetime') \
    .withColumn('tweet_in_golden_window',
                f.when(f.col('tweet_datetime').between(*gw1) |
                       f.col('tweet_datetime').between(*gw2) |
                       f.col('tweet_datetime').between(*gw3) |
                       f.col('tweet_datetime').between(*gw4),1).otherwise(0)) \
    .withColumn('golden_window',
                f.when(f.col('tweet_datetime').between(*gw1),1)
                .when(f.col('tweet_datetime').between(*gw2),2)
                .when(f.col('tweet_datetime').between(*gw3),3)
                .when(f.col('tweet_datetime').between(*gw4),4).otherwise(0))

tweets_sample.filter(f.col('tweet_in_golden_window')==1).show()

# generate DataFrame for total number of tweets for each twitter handle
total_nbr_tweets = tweets_sample \
    .groupBy('twitter_handle_id') \
    .count() \
    .withColumnRenamed('count','total_nbr_tweets')

# generate DataFrame for total number of tweets in golden windows for each twitter handle
total_nbr_tweets_in_gw = tweets_sample \
    .filter(tweets_sample['tweet_in_golden_window']==1) \
    .groupBy('twitter_handle_id') \
    .count() \
    .withColumnRenamed('count','total_nbr_tweets_in_golden_window')

# generate DataFrame for total number golden window hits for each twitter handle
nbr_golden_window_hit = tweets_sample \
    .select('twitter_handle_id','golden_window') \
    .filter(f.col('golden_window')!=0) \
    .dropDuplicates() \
    .groupBy('twitter_handle_id') \
    .count() \
    .withColumnRenamed('count','nbr_golden_window_hit')

# check that rows are OK
total_nbr_tweets.shape()
total_nbr_tweets_in_gw.shape()
nbr_golden_window_hit.shape()
tweets_sample.dropDuplicates(['twitter_handle_id']).shape()

# merge all the columns together and generate WTF index for each twitter handle
wtf_tweets = total_nbr_tweets \
    .join(tweets_sample.select('twitter_handle_id','twitter_handle_name').dropDuplicates(['twitter_handle_id']),on='twitter_handle_id',how='left') \
    .join(total_nbr_tweets_in_gw,on='twitter_handle_id',how='left') \
    .join(nbr_golden_window_hit,on='twitter_handle_id',how='left') \
    .fillna({'total_nbr_tweets_in_golden_window':'0','nbr_golden_window_hit':'0'}) \
    .withColumn('wtf_tweets',(f.col('total_nbr_tweets_in_golden_window')/f.col('total_nbr_tweets'))*(f.col('nbr_golden_window_hit')/len(events))) \
    .sort(f.col('wtf_tweets').desc())\
    .select('twitter_handle_name','twitter_handle_id','total_nbr_tweets','total_nbr_tweets_in_golden_window','nbr_golden_window_hit','wtf_tweets')

wtf_tweets.show()
wtf_tweets.shape()


#:::::::::::::::::::::::::::::::::::::
# > Calculate WTF for news data
#:::::::::::::::::::::::::::::::::::::

# read in news articles
news = spark.read \
    .format("parquet") \
    .load(my_news_source_file) \
    .withColumnRenamed('date_publish','date_publish_str') \
    .withColumn('date_publish', f.col('date_publish_str').cast(TimestampType())) \
    .select('source_domain','title','description','date_publish')

news.show()
news.printSchema()


news_sample = news \
    .where(f.col('date_publish').between(*sample_periods)) \
    .dropDuplicates(['date_publish', 'title', 'description', 'source_domain']) \
    .select('source_domain','date_publish') \
    .withColumn('publish_in_golden_window',
                f.when(f.col('date_publish').between(*gw1) |
                       f.col('date_publish').between(*gw2) |
                       f.col('date_publish').between(*gw3) |
                       f.col('date_publish').between(*gw4),1).otherwise(0)) \
    .withColumn('golden_window',
                f.when(f.col('date_publish').between(*gw1),1)
                .when(f.col('date_publish').between(*gw2),2)
                .when(f.col('date_publish').between(*gw3),3)
                .when(f.col('date_publish').between(*gw4),4).otherwise(0))

news_sample.show()
news.show()
news.describe().show()
date_min = news.agg({"date_publish": "min"}).collect()[0]
print(date_min)
date_max = news.agg({"date_publish": "max"}).collect()[0]
print(date_max)
news_sample.groupBy(news_sample['source_domain']).count().sort(f.col('count').desc()).show()

