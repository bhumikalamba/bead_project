import tweepy
from tweepy import API
from tweepy import Cursor
import pymongo
from datetime import datetime, timedelta
import logging

import sys

logging.basicConfig(level = logging.INFO)


class IngestHistoricalData(object):
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["TwitterDB"]
    mycol = mydb["tweetsData"]

    search_word = '#bitcoin OR #btn OR #blockchain OR #cryptocurrency OR #cryptocurrencies OR #crypto OR #satoshi -filter:retweets'

    def get_tweets(self,api,start_date,end_date):
        tweets = Cursor(api.search, q=self.search_word, lang='en', since=start_date, until=end_date).items(10)
        return tweets

    def save_tweets(self,tweets):
        for tweet in tweets:

            json_data = {
                'tweet_id': tweet._json.get('id'),
                'twitter_handle_name': tweet._json.get('user').get('screen_name'),
                'twitter_handle_id': tweet._json.get('user').get('id'),
                'twitter_datetime': tweet._json.get('created_at'),
                'twitter_handle_desc': tweet._json.get('user').get('description')
            }
            print('twee?',json_data)
            self.mycol.insert_one(json_data)


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


