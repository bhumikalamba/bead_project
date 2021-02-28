from pyspark.sql import SparkSession, SQLContext

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


my_spark = SparkSession \
    .builder \
    .appName("myApp") \
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/TwitterDB.tweetsData") \
    .config("spark.mongodb.output.uri", "mongodb:/127.0.0.1/TwitterDB.tweetsData") \
    .config('spark.jars.packages','org.mongodb.spark:mongo-spark-connector_2.11:2.2.2')\
    .getOrCreate()


df = my_spark.read.format("com.mongodb.spark.sql.DefaultSource").load()
df.printSchema()

print(df.count())

#df.show(4)

