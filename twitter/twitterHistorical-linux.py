import tweepy
from tweepy import API
from tweepy import Cursor
#import pymongo
from datetime import datetime, timedelta
import logging
from google.cloud import bigquery
import os
import sys

logging.basicConfig(level = logging.INFO)


class IngestHistoricalData(object):
    cwd = os.getcwd()
    service_account_file_path = cwd + '/bead-project-18e42f4e742d.json'
    rows_to_insert = []
    bigquery_client = bigquery.Client.from_service_account_json(service_account_file_path)
    table_id = "nice-forge-305606.twitter_dataset.tweets_data"



    search_word = '#bitcoin OR #btn OR #blockchain OR #cryptocurrency OR #cryptocurrencies OR #crypto OR #satoshi -filter:retweets'

    def stream_tweets_to_bigquery(self,api,start_date,end_date):
       count = 0
       for tweet in tweepy.Cursor(api.search, q=self.search_word, count=100, lang='en',since=start_date, until=end_date).items():
           json_data = {
               u'tweet_id': tweet._json.get('id'),
               u'twitter_handle_name': tweet._json.get('user').get('screen_name'),
               u'twitter_handle_id': tweet._json.get('user').get('id'),
               u'twitter_datetime': tweet._json.get('created_at'),
               u'twitter_handle_desc': tweet._json.get('user').get('description')
           }
           self.rows_to_insert.append(json_data)
           count = count + 1
           if count % 1000 == 0:
               self.insert_to_bigquery(self.rows_to_insert)
               self.rows_to_insert = []

    def get_tweets(self,api,start_date,end_date):
        tweets = Cursor(api.search, q=self.search_word, lang='en', since=start_date, until=end_date).items(1000)
        return tweets

    def insert_to_bigquery(self,rows_to_insert):
        errors = self.bigquery_client.insert_rows_json(self.table_id, rows_to_insert)
        if errors == []:
            print("New rows have been added.")
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



