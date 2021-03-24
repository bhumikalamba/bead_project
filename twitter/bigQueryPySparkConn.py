from pyspark.sql import SparkSession
from google.cloud import bigquery
from google.oauth2 import service_account
#credentials = service_account.Credentials.from_service_account_file('C:/Users/suren/Documents/bead_project/bead-project-18e42f4e742d.json')
credentials = service_account.Credentials.from_service_account_file('C:/Users/suren/Documents/bead_project/nice-forge-305606-0c1b603cf119.json')



project_id = 'nice-forge-305606'
client = bigquery.Client(credentials= credentials,project=project_id)

QUERY = (
    'SELECT DISTINCT(twitter_handle_id),twitter_handle_name FROM `nice-forge-305606.twitter_dataset.tweets_data` ')

query_job = client.query(QUERY)  # API request
rows = query_job.result()  # Waits for query to finish



for row in rows:
    print(row.twitter_handle_id)


#
# print(client)
#
# spark = SparkSession \
#   .builder \
#   .appName('spark-bigquery-demo') \
#   .config('spark.jars.packages', 'com.google.cloud.spark:spark-bigquery-with-dependencies_2.11:0.15.1-beta') \
#   .getOrCreate()
#
# # bucket = "nice-forge-305606"
# # spark.conf.set('temporaryGcsBucket', bucket)
#
# table = "nice-forge-305606:twitter_dataset.tweets_data"
# df = spark.read.format("bigquery") \
#   .option('table', table) \
#   .load()
#
# print(df)

# words = spark.read.format('bigquery') \
#   .option('table', 'twitter_dataset.tweets_data') \
#   .load()
#
# print(words)