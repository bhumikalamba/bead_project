import tweepy
from tweepy import API
from tweepy import Cursor
import pymongo
from datetime import datetime, timedelta
import logging
from google.cloud import bigquery

import sys

logging.basicConfig(level = logging.INFO)


class IngestHistoricalData(object):
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["TwitterDB"]
    mycol = mydb["tweetsData"]

    bigquery_client = bigquery.Client()
    table_id = "bead-project.twitter_dataset.tweets_dataset"



    search_word = '#bitcoin OR #btn OR #blockchain OR #cryptocurrency OR #cryptocurrencies OR #crypto OR #satoshi -filter:retweets'

    def get_tweets(self,api,start_date,end_date):
        tweets = Cursor(api.search, q=self.search_word, lang='en', since=start_date, until=end_date).items(10)
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
            if count % 1000 == 0:
                self.insert_to_bigquery(rows_to_insert)
                rows_to_insert = []


if __name__ == "__main__":
    print(sys.argv)
    arg_list = str(sys.argv)
    print(arg_list)

    CONSUMER_KEY = sys.argv[1]
    CONSUMER_SECRET = sys.argv[2]

    ACCESS_TOKEN = sys.argv[3]
    ACCESS_SECRET = sys.argv[4]

    start_date = sys.argv[5]
    NAME = sys.argv[6]
    _end_date = datetime.strptime(start_date,'%Y-%m-%d') + timedelta(1)
    end_date = _end_date.strftime('%Y-%m-%d')

    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)
    api = API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
    ingest = IngestHistoricalData()
    tweets = ingest.get_tweets(api,start_date,end_date)
    ingest.save_tweets(tweets)
    logging.info('Tweets between %s and %s were successfully extracted using %s Twitter details!' % (start_date,end_date,NAME))


