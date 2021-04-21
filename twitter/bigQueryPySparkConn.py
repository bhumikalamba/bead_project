from pyspark.sql import SparkSession
import google.auth
from google.cloud import bigquery

from google.oauth2 import service_account
#credentials = service_account.Credentials.from_service_account_file('C:/Users/suren/Documents/bead_project/bead-project-18e42f4e742d.json')
GOOGLE_APPLICATION_CREDENTIALS = './direct-analog-308416-f082eab9c7fa.json'
cred = google.auth.load_credentials_from_file(GOOGLE_APPLICATION_CREDENTIALS)



project_id = 'direct-analog-308416'
client = bigquery.Client(credentials= cred[0],project=cred[1])

print(client)

spark = SparkSession \
  .builder \
  .appName('spark-bigquery-demo') \
  .config('spark.jars.packages', 'com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.15.1-beta') \
  .getOrCreate()

bucket = "tweets-bucker"
spark.conf.set('temporaryGcsBucket', bucket)

print("heyeeeeee")
table =  "direct-analog-308416.project_data.tweets_data"
df = spark.read.format("bigquery") \
    .option('credentialsFile',GOOGLE_APPLICATION_CREDENTIALS)\
    .option('table', 'direct-analog-308416.project_data.twitter_data_ingest') \
    .load()


print(df)
df.createOrReplaceTempView('df')

df2 = spark.sql(
    'SELECT tweet_id FROM df WHERE followers_count < 50')

df2.show()

# words = spark.read.format('bigquery') \
#   .option('table', 'twitter_dataset.tweets_data') \
#   .load()
#
# print(words)
