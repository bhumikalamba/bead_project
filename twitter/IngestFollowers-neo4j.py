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
import logging
from GetFollowers import *

logging.basicConfig(level=logging.INFO)


os.environ['BEARER_TOKEN'] ='AAAAAAAAAAAAAAAAAAAAAJtxMQEAAAAAJZmeOOGETISoJvjAbS1loA3BU0A%3DA3Qdf8LFDm81fyl6rkKd2W1AfHGbkEXYRctvW7zumvsTLmp9nT'
#os.environ['BEARER_TOKEN'] ='AAAAAAAAAAAAAAAAAAAAAJxRNQEAAAAAd9lCTHl5MjHWqnQnPxAvvpUkhU4%3DD5ywyPyd8fsdCFwfITvIEaWy0WK2OP3Bq7hg58LkWU1B9JwkUc'

GOOGLE_APPLICATION_CREDENTIALS="C:/Users/Suren/Documents/nice-forge-305606-0c1b603cf119.json"

spark = SparkSession.builder.appName('READ twitter CSV files').getOrCreate()

from pyspark.sql.types import StructType, StructField, StringType,IntegerType

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

df = spark.read.format('csv') \
    .option("inferSchema","true") \
    .option("header","true") \
    .option("sep",",") \
    .load("bq-results-ingested.csv")

df.printSchema()



## Drop duplicate tweets
df2 = df.dropDuplicates().agg(F.expr('percentile(followers_count, array(0.5))')[0].alias('50-percentile'),F.expr('percentile(followers_count, array(0.75))')[0].alias('75-percentile'))
df_filtered = df.filter((df['followers_count'] > int(df2.select("50-percentile").first()[0])) & (df['followers_count'] < int(df2.select("75-percentile").first()[0]))).dropDuplicates(['twitter_handle_id'])

logging.info('The script will run for approximately %s hours' % str((df_filtered.select(F.sum('followers_count')).collect()[0][0]/ 75000) * 15/60))


user_lists = df_filtered.select(["twitter_handle_id","twitter_handle_name"]).rdd.flatMap(lambda x: [x]).collect()




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
    print(response.status_code)
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
    print('url',url)
    try:
        main_node = User.get_or_create({"id_str":user_id[0]})
    except Exception as ex:
        print('dafuq',ex)
    print(main_node)
    main_node[0].screen_name = user_id[1]
    main_node[0].save()
    headers = create_headers(bearer_token)
    params = get_params(nextcursor)
    print('partam',params)
    json_response = connect_to_endpoint(url, headers, params)
    print('json resp',json_response)
    create_graph(json_response,main_node)
    #print(json.dumps(json_response, indent=4, sort_keys=True)
    export_data_to_json(json_response, "followers{}.json".format(currfilecount))


for user in user_lists:
    # print('user',user[0])
    # print('user woho', user[1])
    # print(stopit)
    # create folder
    # os.makedirs(str(user))
    user_id = user
    # set up first file count
    currfilecount = 1
    # nextcursor "-1" refers to first results page
    nextcursor = -1
    try:
        # get first page of results & export to json
        print('hey')
        #main()
        begin_ingestion(user_id)
        print('afer main')
    except:
        # if unable to get first page of results, sleep and retry
        logging.info('sleeping for 15 mins')
        time.sleep(60 * 15)
        logging.info('Okay! Back to work!')
        main()
    finally:
        print('File1 for user_id {} exported. Loading File1...'.format(user_id[0]))

    # load followers1.json into data
    # data = load_data_from_json("followers{}.json".format(currfilecount))

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
            main()
        finally:
            print("File followers {} exported!".format(currfilecount))
    print("Extraction completed for user_id {}... Moving files".format(user_id[0]))
    files = os.listdir()
    dest = os.getcwd() + "\\{}".format(user[0])
    for f in files:
        if (f.startswith("followers")):
            shutil.move(f, dest)
    print("files moved into user_id {} folder".format(user_id[0]))


