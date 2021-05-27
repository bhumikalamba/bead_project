from pyspark.sql import SparkSession
import logging
import json
import csv
import os


directory = os.getcwd()
edge_dir = directory + '\edges_latest_test'
for root,dirs,files in os.walk(edge_dir):
    for file in files:
        if file.endswith(".csv"):
            file = edge_dir + '\\' + str(file)
            print(file)



logging.basicConfig(level=logging.INFO)

spark = SparkSession.builder.\
        appName('READ Neo4j data')\
        .config('spark.jars.packages', 'com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.15.1-beta,com.google.cloud.bigdataoss:gcs-connector:hadoop3-1.9.18')\
        .config('spark.hadoop.fs.gs.impl','com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')\
        .config('spark.jars.packages', 'neo4j-contrib:neo4j-connector-apache-spark_2.11:4.0.1')\
        .getOrCreate()

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


btc_tweeted_users = spark.read.format("org.neo4j.spark.DataSource") \
  .option("url", "bolt://34.87.46.194:7687") \
  .option("authentication.type", "basic") \
  .option("authentication.basic.username", "neo4j") \
  .option("authentication.basic.password", "$martBEAD&") \
  .option("query",
          "MATCH (n:User) WHERE n.screen_name CONTAINS '' RETURN n.screen_name as screen_name,n.id_str as id,n.description as description,n.followers_count as followers_count")\
  .load()

btc_tweeted_users_relations = btc_tweeted_users.join(src_tgt_df, btc_tweeted_users.id == src_tgt_df['`source.id_str`'], how='left').na.drop(subset=["`<rel.type>`"])
vertex = btc_tweeted_users_relations.select("id","screen_name","description").selectExpr("id as ID","screen_name as SCREEN_NAME")
edges = btc_tweeted_users_relations.select("id", "`target.id_str`").selectExpr("id as START_ID", "`target.id_str` as END_ID")

json_list = list()

edges.write.csv('edges_latest_test1')
vertex.write.csv('vertex_latest_test1')

json_list = list()
edge_rows = []
vertex_rows = []
vertex_ids = []

directory = os.getcwd()

edge_dir = directory + '\edges_latest_test1'
for root,dirs,files in os.walk(edge_dir):
    for file in files:
        if file.endswith(".csv"):
            #file = '.\edges_latest_test1' + str(file)
            file = edge_dir + '\\' + str(file)
            edges_file = open(file, 'r')
            #  perform calculation
            edges_reader = csv.reader(edges_file, delimiter=',')
            for row in edges_reader:
                edge_rows.append(row)
            edges_file.close()

vertex_dir = directory + '\/vertex_latest_test1'
for root,dirs,files in os.walk(vertex_dir):
    for file in files:
        if file.endswith(".csv"):
            #file = '.\/vertex_latest_test1' + str(file)
            file = vertex_dir + '\\' + str(file)
            vertex_file = open(file, 'r')
            #  perform calculation
            vertex_read = csv.reader(vertex_file)
            for row in vertex_read:
                vertex_ids.append(row[0])
                vertex_rows.append(row)
            vertex_file.close()

# with open('./edges_latest1/part-00000-653f0ec6-5124-4c38-85a7-4e93309f9aa1-c000.csv') as edges_file:
#     edges_reader = csv.reader(edges_file, delimiter=',')
#     for row in edges_reader:
#         edge_rows.append(row)
#
#
# with open('./vertex_latest1/part-00000-2e0aa76e-75b7-45cb-95cb-b6ec1c7873e6-c000.csv') as vertex_file:
#     vertex_read = csv.reader(vertex_file)
#     for row in vertex_read:
#         vertex_ids.append(row[0])
#         vertex_rows.append(row)

edges_reader = None
vertex_read = None

edges.show()
vertex.show()

for row in edges:
    edge_rows.append(row)

for row in vertex:
    vertex_ids.append(row[0])
    vertex_rows.append(row)

print(vertex)

print(edges)
checked_in_list = []
for r in vertex_rows:
    for row in edge_rows:
        json_val = dict()
        if r[0] == row[0]:
            if len(json_list) == 0:
                json_val['ID'] = str(r[0])
                json_val['Screen_name'] = str(r[1])
                json_val['Follows'] = [str(row[1])]
                json_list.append(json_val)
                checked_in_list.append(r[0])
            else:
                if r[0] in checked_in_list:
                    for val in json_list:
                        if val['ID'] == r[0]:
                            if row[1] not in val['Follows']:
                                val['Follows'].append(row[1])
                else:
                    json_val['ID'] = str(r[0])
                    json_val['Screen_name'] = str(r[1])
                    json_val['Follows'] = [str(row[1])]
                    json_list.append(json_val)
                    checked_in_list.append(r[0])


with open('data-test.json', 'w') as f:
    json.dump(json_list, f)





