from pyspark.sql import SparkSession
import google.auth
from google.cloud import bigquery

from google.oauth2 import service_account
#credentials = service_account.Credentials.from_service_account_file('C:/Users/suren/Documents/bead_project/bead-project-18e42f4e742d.json')
GOOGLE_APPLICATION_CREDENTIALS = 'C:/Users/suren/Documents/bead_project/direct-analog-308416-f082eab9c7fa.json'
cred = google.auth.load_credentials_from_file(GOOGLE_APPLICATION_CREDENTIALS)



project_id = 'direct-analog-308416'
client = bigquery.Client(credentials= cred[0],project=cred[1])

print(client)

spark = SparkSession \
  .builder \
  .appName('spark-bigquery-demo') \
  .config('spark.jars.packages', 'com.google.cloud.spark:spark-bigquery-with-dependencies_2.11:0.15.1-beta') \
  .getOrCreate()

bucket = "tweets-bucker"
spark.conf.set('temporaryGcsBucket', bucket)


table =  "direct-analog-308416.project_data.tweets_data"
df = spark.read.format("bigquery") \
    .option("credentialsFile",GOOGLE_APPLICATION_CREDENTIALS)\
    .option('project','direct-analog-308416')\
    .option('parentProject','direct-analog-308416')\
    .option("dataset", "project_data")\
    .option('table', 'tweets_data') \
    .load()

print('ummm')
df.createOrReplaceTempView('df')

df2 = spark.sql(
    'SELECT * FROM df')
print('mmmu')
print(df2.show())

# words = spark.read.format('bigquery') \
#   .option('table', 'twitter_dataset.tweets_data') \
#   .load()
#
# print(words)