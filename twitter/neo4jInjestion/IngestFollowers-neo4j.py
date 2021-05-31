from pyspark.sql import SparkSession
from google.cloud import bigquery
import pyspark.sql.functions as F
import tweepy
from tweepy import API
import logging
import google.auth
from GetFollowers import *
from pyspark.sql.functions import col
from pyspark.sql.types import *
from google.cloud import bigquery_storage


CONSUMER_KEY = "CONSUMER KEY"
CONSUMER_SECRET = "COSUMER TOKEN"
ACCESS_TOKEN = "ACCESS TOKEN"
ACCESS_SECRET =  "ACCESS SECRET"

tweepy_auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
tweepy_auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)

logging.basicConfig(level=logging.INFO)


os.environ['BEARER_TOKEN'] ='BEARER TOKEN'

GOOGLE_APPLICATION_CREDENTIALS="./direct-analog-308416-f082eab9c7fa.json"
cred = google.auth.load_credentials_from_file(GOOGLE_APPLICATION_CREDENTIALS)
PROJECT_ID = 'direct-analog-308416'
client = bigquery.Client(credentials= cred[0],project=cred[1])

spark = SparkSession.builder.appName('READ From BigQuery').\
    config('spark.jars.packages', 'com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.15.1-beta').getOrCreate()

bqclient = bigquery.Client(credentials=cred[0], project=cred[1],)
bqstorageclient = bigquery_storage.BigQueryReadClient(credentials=cred[0])

query_string = """
SELECT twitter_handle_name,twitter_handle_id,twitter_handle_desc,followers_count,friends_count FROM `direct-analog-308416.project_data.tweets_data`
"""

tweets = (
    bqclient.query(query_string)
    .result()
    .to_dataframe(bqstorage_client=bqstorageclient)
)



###

### USING CSV FILES

# NOTE TO SURENDHAR: use this schema. will parse through the values and use null if the value doesn't make sense.
# tweets_schema = StructType([
#     StructField('tweet_id',StringType(),True),
#     StructField('twitter_handle_name',StringType(),True),
#     StructField('twitter_handle_id',StringType(),True),
#     StructField('tweet_datetime',TimestampType(),True),
#     StructField('twitter_handle_desc',StringType(),True),
#     StructField('followers_count',IntegerType(),True),
#     StructField('friends_count',IntegerType(),True),
#     StructField('listed_count',IntegerType(),True),
#     StructField('created_at_datetime',TimestampType(),True),
#     StructField('tweet_datetime_hour',TimestampType(),True),
#     StructField('statuses_count',IntegerType(),True),
#     StructField('tweet',StringType(),True),
#     StructField('truncated',BooleanType(),True)
# ])
tweets_schema = StructType([
    StructField('twitter_handle_name',StringType(),True),
    StructField('twitter_handle_id',StringType(),True),
    StructField('twitter_handle_desc',StringType(),True),
    StructField('followers_count',FloatType(),True),
    StructField('friends_count',FloatType(),True)
])
df = spark.createDataFrame(tweets,schema=tweets_schema)


# addded option('mode','DROMPMALFORMED') and used the custom schema
# df = spark.read.format('csv')\
#     .option("header","true")\
#     .option('mode','DROPMALFORMED')\
#     .schema(tweets_schema) \
#     .load("bq-results-full.csv")

df.show()
df.printSchema()


## Drop duplicate tweets
df2 = df\
    .select('twitter_handle_name','twitter_handle_id','twitter_handle_desc','followers_count','friends_count')\
    .dropDuplicates()\
    .dropDuplicates(['twitter_handle_id'])\
    .agg(F.expr('percentile(followers_count, array(0.5))')[0].alias('50-percentile'),F.expr('percentile(followers_count, array(0.75))')[0].alias('75-percentile'))

df2.show()


df_filtered = df \
    .select('twitter_handle_name','twitter_handle_id','twitter_handle_desc','followers_count','friends_count') \
    .filter((df['followers_count'] >= int(df2.select("50-percentile").first()[0])) & (df['followers_count'] <= int(df2.select("75-percentile").first()[0])))\
    .dropDuplicates(['twitter_handle_id'])\
    .sort(col("followers_count").desc())

df_filtered.show(10)

user_lists = df_filtered.select(["twitter_handle_id","twitter_handle_name","twitter_handle_desc","followers_count","friends_count"]).rdd.flatMap(lambda x: [x]).collect()


## Below codes connect to twitter API to get the followers ID
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


def create_graph(data,main_node):
    for user in data['ids']:
        user_cre = User.get_or_create({"id_str":user})
        user_cre[0].follows.connect(main_node[0])
        user_cre[0].save()


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
    export_data_to_json(json_response, "followers{}.json".format(currfilecount))

exported_IDs = list()

for file in glob.glob("*twitter*"):
    if '\\' in file:
        exported_IDs.append(file.split('\\')[1])

for user in user_lists:
    user_id = user

    # set up first file count
    api = API(tweepy_auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
    user_exist = True
    logging.info('Loading for user, %s',str(user_id[1]))
    if user_id[0] in exported_IDs:
        user_exist = False

    if user_exist == True and user_id[0].isdigit():
        try:
            user_obj = api.get_user(user_id[0])
            if user_obj.protected == True:
                user_exist = False
        except Exception as ex:
            user_exist = False
            logging.info("User account no longer exists in Twitter. %s",str(user[1]))
    else:
        user_exist = False

    if user_exist:
        currfilecount = 1
        # nextcursor "-1" refers to first results page
        nextcursor = -1
        try:
            # get first page of results & export to json
            begin_ingestion(user_id)
        except Exception as ex:
            print('Exception occured on level 1',ex)
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
            except Exception as ex:
                print('Exception occured on level 2',ex)
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


