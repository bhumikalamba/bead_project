import tweepy
from tweepy import API
from tweepy import Cursor
#import pymongo
from datetime import datetime, timedelta
import logging
from google.cloud import bigquery
import os
import sys
from dateutil import tz

logging.basicConfig(level = logging.INFO)


class IngestHistoricalData(object):
    cwd = os.getcwd()
    service_account_file_path = cwd + '/direct-analog-308416-736bf46c2284.json'
    rows_to_insert = []
    bigquery_client = bigquery.Client.from_service_account_json(service_account_file_path)
    table_id = "direct-analog-308416.project_data.twitter_data_ingest"
    to_zone = tz.gettz('Asia/Singapore')


    search_word = '#btc OR bitcoin OR #blockchain OR #crypto OR #satoshi -filter:retweets'

    def stream_tweets_to_bigquery(self,api,start_date,end_date):
       count = 0
       for tweet in tweepy.Cursor(api.search, q=self.search_word,lang='en',since=start_date, until=end_date).items(500000):
           tweet_datetime = datetime.strptime(tweet._json.get('created_at'), '%a %b %d %H:%M:%S %z %Y').astimezone(self.to_zone).replace(tzinfo=None)
           twitter_datetime_hour = tweet_datetime.replace(microsecond=0, second=0, minute=0)
           created_at_datetime = datetime.strptime(tweet._json.get('user').get('created_at') , '%a %b %d %H:%M:%S %z %Y').replace(tzinfo=None)
           json_data = {
               u'tweet_id': tweet._json.get('id'),
               u'twitter_handle_name': tweet._json.get('user').get('screen_name'),
               u'twitter_handle_id': tweet._json.get('user').get('id'),
               u'tweet_datetime': str(tweet_datetime),
               u'twitter_handle_desc': tweet._json.get('user').get('description'),
               u'followers_count': int(tweet._json.get('user').get('followers_count')),
               u'friends_count': int(tweet._json.get('user').get('friends_count')),
               u'listed_count': int(tweet._json.get('user').get('listed_count')),
               u'created_at_datetime': str(created_at_datetime),
               u'twitter_datetime_hour': str(twitter_datetime_hour),
               u'statuses_count':int(tweet._json.get('user').get('statuses_count')),
               u'tweet':tweet._json.get('text'),
               u'truncated':bool(tweet._json.get('truncated'))
           }
           self.rows_to_insert.append(json_data)
           count = count + 1
           self.insert_to_bigquery(self.rows_to_insert)
           self.rows_to_insert =[]
           if count % 1000 == 0:
               print('1000 rows have been inserted successfully!')


    def get_tweets(self,api,start_date,end_date):
        tweets = Cursor(api.search, q=self.search_word, lang='en', since=start_date, until=end_date).items(1000)
        return tweets

    def insert_to_bigquery(self,rows_to_insert):
        errors = self.bigquery_client.insert_rows_json(self.table_id, rows_to_insert)
        if errors == []:
            pass
            #print("New rows have been added.")
        else:
            print("Encountered errors while inserting rows: {}".format(errors))

    def save_tweets(self,tweets):
        count = 0
        rows_to_insert = []
        for tweet in tweets:
            json_data = {
                u'tweet_id': tweet._json.get('id'),
                u'twitter_handle_name': tweet._json.get('user').get('screen_name'),
                u'twitter_handle_id': tweet._json.get('user').get('id'),
                u'twitter_datetime': tweet._json.get('created_at'),
                u'twitter_handle_desc': tweet._json.get('user').get('description')
            }
            rows_to_insert.append(json_data)
            count = count + 1
            if count % 10 == 0:
                self.insert_to_bigquery(rows_to_insert)
                rows_to_insert = []



