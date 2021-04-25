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

spark = SparkSession\
         .builder \
         .appName('READ Neo4j data') \
         .config('spark.jars.packages', 'neo4j-contrib:neo4j-connector-apache-spark_2.11:4.0.1') \
         .getOrCreate()

## change localhost to 34.87.46.194 when deploying
'''
This pulls the relationships
  .option("relationship","FOLLOWS") \
  .option("relationship.nodes.map", "true")\
  .option("relationship.source.labels", "User")\
  .option("relationship.target.labels", "User")\
'''

'''
this pulls the executes the query and pulls the data

.option("query", "MATCH (n:User) WHERE n.screen_name CONTAINS '' RETURN n.screen_name,n.id_str,n.description,n.followers_count")\
'''
df = spark.read.format("org.neo4j.spark.DataSource") \
  .option("url", "bolt://localhost:7687") \
  .option("authentication.type", "basic") \
  .option("authentication.basic.username", "neo4j") \
  .option("authentication.basic.password", "password") \
  .option("relationship","FOLLOWS") \
  .option("relationship.nodes.map", "true")\
  .option("relationship.source.labels", "User")\
  .option("relationship.target.labels", "User")\
  .load()

df.show()

#   .option("query", "MATCH (n:User) WHERE n.screen_name CONTAINS '' RETURN n.screen_name,n.id_str,n.description,n.followers_count")\
