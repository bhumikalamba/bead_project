from pyspark.sql import SparkSession
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import pytz
from itertools import cycle
import numpy as np
from google.cloud import bigquery
from google.oauth2 import service_account
import pyspark.sql.functions as F
from pyspark import *
import tweepy
from tweepy import API
import logging
from GetFollowers import *
from pyspark.sql.functions import col


CONSUMER_KEY = "7dJU7wZSHhZwTNXshgczruB7Q"
CONSUMER_SECRET = "tbYF5yp3S0LjNIeV5XMkA33hhuYeuvYOuVVy1X6fgGxJeygWUp"
ACCESS_TOKEN = "313172786-DtYVsYbcWXjDZMGuDIJY3tU8dm1Ax0oXl03RQ4Uz"
ACCESS_SECRET =  "aBjwOguGu2bthYjC3xXxSOSbLac1C410B0o4pT9Akfir8"

tweepy_auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
tweepy_auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)

logging.basicConfig(level=logging.INFO)


os.environ['BEARER_TOKEN'] ='AAAAAAAAAAAAAAAAAAAAAJtxMQEAAAAAJZmeOOGETISoJvjAbS1loA3BU0A%3DA3Qdf8LFDm81fyl6rkKd2W1AfHGbkEXYRctvW7zumvsTLmp9nT'
#os.environ['BEARER_TOKEN'] ='AAAAAAAAAAAAAAAAAAAAAJxRNQEAAAAAd9lCTHl5MjHWqnQnPxAvvpUkhU4%3DD5ywyPyd8fsdCFwfITvIEaWy0WK2OP3Bq7hg58LkWU1B9JwkUc'

GOOGLE_APPLICATION_CREDENTIALS="C:/Users/Suren/Documents/nice-forge-305606-0c1b603cf119.json"

spark = SparkSession.builder.appName('READ twitter CSV files').getOrCreate()

# NOTE TO SURENDHAR: changed to import *
from pyspark.sql.types import *

### BIG QUERY CONNECTION CHECK OUT LATER

# credentials = service_account.Credentials.from_service_account_file('C:/Users/suren/Documents/bead_project/direct-analog-308416-f082eab9c7fa.json')
# project_id = 'direct-analog-308416'
# client = bigquery.Client(credentials= credentials,project=project_id)
# QUERY = (
#     'SELECT * FROM `direct-analog-308416.project_data.twitter_data_ingest` ')
# query_job = client.query(QUERY)  # API request
# rows = query_job.result()  # Waits for query to finish
#
# print('type',type(rows))
#
# print(rows)

###

### USING CSV FILES

# NOTE TO SURENDHAR: use this schema. will parse through the values and use null if the value doesn't make sense.
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

# NOTE TO SURENDHAR: addded option('mode','DROMPMALFORMED') and used the custom schema
df = spark.read.format('csv')\
    .option("header","true")\
    .option('mode','DROPMALFORMED')\
    .schema(tweets_schema) \
    .load("bq-results-full.csv")

df.show()
df.printSchema()


## Drop duplicate tweets
# NOTE TO SURENDHAR: I selected just columns at the twitter ID level first before de-dup.
df2 = df\
    .select('twitter_handle_name','twitter_handle_id','twitter_handle_desc','followers_count','friends_count')\
    .dropDuplicates()\
    .dropDuplicates(['twitter_handle_id'])\
    .agg(F.expr('percentile(followers_count, array(0.5))')[0].alias('50-percentile'),F.expr('percentile(followers_count, array(0.75))')[0].alias('75-percentile'))

df2.show()

# NOTE TO SURENDHAR: Using >= and <=
df_filtered = df \
    .select('twitter_handle_name','twitter_handle_id','twitter_handle_desc','followers_count','friends_count') \
    .filter((df['followers_count'] >= int(df2.select("50-percentile").first()[0])) & (df['followers_count'] <= int(df2.select("75-percentile").first()[0])))\
    .dropDuplicates(['twitter_handle_id'])\
    .sort(col("followers_count").desc())

df_filtered.show(50)

logging.info('The script will run for approximately %s hours' % str((df_filtered.select(F.sum('followers_count')).collect()[0][0]/ 75000) * 15/60))

user_lists = df_filtered.select(["twitter_handle_id","twitter_handle_name","twitter_handle_desc","followers_count","friends_count"]).rdd.flatMap(lambda x: [x]).collect()




def auth():
    return os.environ.get("BEARER_TOKEN")

def create_url(user_id):
    return "https://api.twitter.com/1.1/followers/ids.json?user_id={}".format(user_id)

def get_params(nextcursor):
    return{"count":5000, "cursor":nextcursor}
# default cursor =  -1, refering to first page of results
# example https://api.twitter.com/1.1/followers/ids.json?cursor=-1&screen_name=andypiper&count=5000

def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers


def connect_to_endpoint(url, headers, params):
    response = requests.request("GET", url, headers=headers, params=params)
    if response.status_code != 200:
        raise Exception(
            "Request returned an error: {} {}".format(
                response.status_code, response.text
            )
        )
    return response.json()

def export_data_to_json(data, filename):
    # save to json file
    with open(filename, 'w') as fp:
        json.dump(data, fp)


def load_data_from_json(filename):
    with open(filename, 'r') as fp:
        data = json.load(fp)
    return data

# def export_else_append(data, filename):
#     if os.path.isfile(filename):
#         print("File exist & appended")
#         apppend_data_to_json(data, filename)
#     else:
#         print("File not exist. New File created.")
#         export_data_to_json(data, filename)

def create_graph(data,main_node):
    for user in data['ids']:
        user_cre = User.get_or_create({"id_str":user})
        user_cre[0].follows.connect(main_node[0])
        user_cre[0].save()

#def main():
def begin_ingestion(user_id):
    bearer_token = auth()
    url = create_url(user_id[0])
    main_node = User.get_or_create({"id_str":user_id[0]})
    main_node[0].screen_name = user_id[1]
    main_node[0].description = user_id[2]
    main_node[0].followers_count = user_id[3]
    main_node[0].friends_count = user_id[4]
    main_node[0].save()
    headers = create_headers(bearer_token)
    params = get_params(nextcursor)
    json_response = connect_to_endpoint(url, headers, params)
    create_graph(json_response,main_node)
    #print(json.dumps(json_response, indent=4, sort_keys=True)
    export_data_to_json(json_response, "followers{}.json".format(currfilecount))


for user in user_lists:
    user_id = user
    # set up first file count
    api = API(tweepy_auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
    user_exist = True
    try:
        user_obj = api.get_user(user_id[0])
        if user_obj.protected == True:
            user_exist = False
    except Exception as ex:
        user_exist = False
        logging.info("User account no longer exists in Twitter.")

    if user_exist:
        currfilecount = 1
        # nextcursor "-1" refers to first results page
        nextcursor = -1
        try:
            # get first page of results & export to json
            begin_ingestion(user_id)
        except Exception as ex:
            print('what uh', ex)
            # if unable to get first page of results, sleep and retry
            logging.info('sleeping for 15 mins')
            time.sleep(60 * 15)
            logging.info('Okay! Back to work!')
            #main()
            begin_ingestion(user_id)
        finally:
            print('File1 for user_id {} exported. Loading File1...'.format(user_id[0]))

        # load followers1.json into data
        data = load_data_from_json("followers{}.json".format(currfilecount))

        print('Getting subsequent files...')
        while nextcursor != 0:
            # load latest followers().json
            data = load_data_from_json("followers{}.json".format(currfilecount))
            # based on latest json file loaded, set up nextcursor; For the next API call.
            nextcursor = data['next_cursor']
            # add one count to currfilecount; For exporting to a new json in the next main() run.
            currfilecount += 1
            try:
                # call API with updated nextcursor and export to new json file
                #main()
                begin_ingestion(user_id)
            except:
                # sleep for 15 minutes if error, and try again
                logging.info('Sleeping for 15 mins')
                time.sleep(60 * 15)
                logging.info('Okay! Back to work!')
                #main()
                begin_ingestion(user_id)
            finally:
                print("File followers {} exported!".format(currfilecount))
        print("Extraction completed for user_id {}... Moving files".format(user_id[0]))
        files = os.listdir()
        dest = os.getcwd() + "\\{}".format(user[0])
        for f in files:
            if (f.startswith("followers")):
                shutil.move(f, dest)
        print("files moved into user_id {} folder".format(user_id[0]))


