from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession \
    .builder \
    .appName("parse keywords and language") \
    .getOrCreate()

sc = spark.sparkContext

articles = spark.read\
    .format("orc")\
    .load("/Users/rais/Desktop/post grad/EBAC/2020/CA/canary_v1/news-please/cc_store/20210221195808/*")

articles.describe().show()

articles.show()


articles = articles.dropDuplicates(["authors", "date_publish", "title", "description", "source_domain"])
articles.count()

articles.groupBy(articles["source_domain"]).count().show()

articles.groupBy("source_domain","authors")\
    .count()\
    .sort("source_domain",col("count").desc())\
    .show()
