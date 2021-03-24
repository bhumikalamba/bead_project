import os
from datetime import datetime, timedelta
import json
from twitterHistorical import IngestHistoricalData
import tweepy
from tweepy import API
import logging

logging.basicConfig(level=logging.INFO)

cwd = os.getcwd()

_start_date = '2021-03-03'  ## Amend start date for a different timeperiod
json_data = None

json_file_path = cwd + "/" + "twitterTokens.json"

with open(json_file_path) as json_file:
    json_data = json.load(json_file)

if json_data is not None:
    for token_detail in json_data:
        start_date = datetime.strptime(_start_date, '%Y-%m-%d')
        start_date_str = start_date.strftime('%Y-%m-%d')
        CONSUMER_KEY = str(token_detail.get('CONSUMER_KEY'))
        CONSUMER_SECRET = str(token_detail.get('CONSUMER_SECRET'))

        ACCESS_TOKEN = str(token_detail.get('ACCESS_TOKEN'))
        ACCESS_SECRET = str(token_detail.get('ACCESS_SECRET'))
        _end_date = datetime.strptime(start_date_str, '%Y-%m-%d') + timedelta(1)
        end_date = _end_date.strftime('%Y-%m-%d')

        auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
        auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)
        api = API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

        ingest = IngestHistoricalData()
        ingest.stream_tweets_to_bigquery()
        logging.info('Tweets between %s and %s were successfully extracted using %s Twitter details!' % (start_date_str, end_date, str(token_detail.get('NAME'))))
        # os.system(call_script)
        _start_date = datetime.strptime(start_date_str, '%Y-%m-%d') + timedelta(1)
        _start_date = _start_date.strftime('%Y-%m-%d')

