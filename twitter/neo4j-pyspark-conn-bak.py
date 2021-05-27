from pyspark.sql import SparkSession
import logging
import google.auth
from closeness_centrality import *
from graphframes import GraphFrame
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO)

GOOGLE_APPLICATION_CREDENTIALS = './direct-analog-308416-f082eab9c7fa.json'
cred = google.auth.load_credentials_from_file(GOOGLE_APPLICATION_CREDENTIALS)


spark = SparkSession\
         .builder \
         .master('yarn')\
         .appName('READ Neo4j data') \
         .config('spark.jars.packages', 'com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.15.1-beta') \
         .config('spark.hadoop.fs.gs.impl','com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')\
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
src_tgt_df = spark.read.format("org.neo4j.spark.DataSource") \
  .option("url", "bolt://34.87.46.194:7687") \
  .option("authentication.type", "basic") \
  .option("authentication.basic.username", "neo4j") \
  .option("authentication.basic.password", "$martBEAD&") \
  .option("relationship","FOLLOWS") \
  .option("relationship.nodes.map", "false")\
  .option("relationship.source.labels", "User")\
  .option("relationship.target.labels", "User")\
  .load()

#df.select("`source.id_str`").show(300,truncate=False)
#df.select('`<source>`.`id_str`').show(300,truncate=False)
#df.select('`<target>`.`id_str`').show(5,truncate=False)
#df.select('`<target>`.`id_str`').show(20,truncate=False)
#df.where('`<target>`.`screen_name` == ""').show()
# print('HEYY')
#edges = df.select("`<rel.type>`","`source.id_str`","`target.id_str`").selectExpr("`<rel.type>` as relationship", "`source.id_str` as src", "`target.id_str` as dst")


btc_tweeted_users = spark.read.format("org.neo4j.spark.DataSource") \
  .option("url", "bolt://34.87.46.194:7687") \
  .option("authentication.type", "basic") \
  .option("authentication.basic.username", "neo4j") \
  .option("authentication.basic.password", "$martBEAD&") \
  .option("query",
          "MATCH (n:User) WHERE n.screen_name CONTAINS '' RETURN n.screen_name as screen_name,n.id_str as id,n.description as description,n.followers_count as followers_count")\
  .load()




btc_tweeted_users_relations = btc_tweeted_users.join(src_tgt_df, btc_tweeted_users.id == src_tgt_df['`source.id_str`'], how='left').na.drop(subset=["`<rel.type>`"])

btc_tweeted_users_relations.show()

vertex = btc_tweeted_users_relations.select("screen_name","id","description","followers_count")
edges = btc_tweeted_users_relations.select("id", "`target.id_str`","`<rel.type>`").selectExpr("`<rel.type>` as relationship", "id as src", "`target.id_str` as dst")


logging.info('Building graphframe')
g = GraphFrame(vertex,edges)

## Degree centrality

logging.info('Applying degree centrality')
total_degree = g.degrees
in_degree = g.inDegrees
out_degree = g.outDegrees

project_id = 'direct-analog-308416'
client = bigquery.Client(credentials= cred[0],project=cred[1])

deg_centrality = total_degree.join(in_degree, "id", how="left")\
    .join(out_degree, "id", how="left")\
    .fillna(0)\
    .sort("inDegree", ascending=False)

deg_centrality.show()

## save degree centrality results to BQ

deg_centrality_table = 'direct-analog-308416:project_data.degree_centrality_results'

deg_centrality.write.format('bigquery')\
    .mode('overwrite')\
    .option('table',deg_centrality_table)\
    .save()

## closeness centrality

vertices = g.vertices.withColumn("ids", F.array())
cached_vertices = AM.getCachedDataFrame(vertices)
g2 = GraphFrame(cached_vertices, g.edges)

for i in range(0, g2.vertices.count()):
    msg_dst = new_paths_udf(AM.src["ids"], AM.src["id"])
    msg_src = new_paths_udf(AM.dst["ids"], AM.dst["id"])
    agg = g2.aggregateMessages(F.collect_set(AM.msg).alias("agg"),
    sendToSrc=msg_src, sendToDst=msg_dst)
    res = agg.withColumn("newIds", flatten_udf("agg")).drop("agg")
    new_vertices = (g2.vertices.join(res, on="id", how="left_outer")
                    .withColumn("mergedIds", merge_paths_udf("ids", "newIds",
                    "id")).drop("ids", "newIds")
                    .withColumnRenamed("mergedIds", "ids"))
    cached_new_vertices = AM.getCachedDataFrame(new_vertices)
    g2 = GraphFrame(cached_new_vertices, g2.edges)

closeness_centrality = g2.vertices\
    .withColumn("closeness", closeness_udf("ids"))\
    .sort("closeness", ascending=False)

closeness_centrality.show()


## save closeness centrality results to BQ

closeness_centrality_table = 'direct-analog-308416:project_data.closeness_centrality'

closeness_centrality.write\
    .format('bigquery')\
    .mode('overwrite')\
    .option('table',closeness_centrality_table)\
    .save()

"""
Pagerank
damping factor = 1 - resetProbability
damping factor defines the probability that the next click will be through a link (webpage context)
"""

results = g.pageRank(resetProbability=0.15, maxIter=20)
#results.vertices.select("id", "pagerank").show()
pagerank_res = results.vertices.sort("pagerank",ascending=False).show()

## save pagerank results to BQ

pagerank_table = 'direct-analog-308416:project_data.pagerank_results'
pagerank_res.write\
    .format('bigquery')\
    .mode('overwrite')\
    .option('table',pagerank_table)\
    .save()

#   .option("query", "MATCH (n:User) WHERE n.screen_name CONTAINS '' RETURN n.screen_name,n.id_str,n.description,n.followers_count")\

"""
Community Detection
Triangle indicates that two of a node's neighbours are also neighbours value of 1 would mean the user is a part of a triangle
"""
logging.info('Triangle count algorithm')
result = g.triangleCount()
triangle_count = result.sort("count", ascending=False).filter('count > 0')
triangle_count.show()


## save triangle count results to BQ

triangle_count_table = 'direct-analog-308416:project_data.triangle_count_results'
triangle_count.write\
    .format('bigquery')\
    .mode('overwrite')\
    .option('table',triangle_count_table)\
    .save()