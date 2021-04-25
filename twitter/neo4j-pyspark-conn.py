from pyspark.sql import SparkSession


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
  .option("relationship.nodes.map", "false")\
  .option("relationship.source.labels", "User")\
  .option("relationship.target.labels", "User")\
  .load()


edges = df.select("`<rel.type>`","`source.id_str`","`target.id_str`").selectExpr("`<rel.type>` as relationship", "`source.id_str` as src", "`target.id_str` as dst")


vertex = spark.read.format("org.neo4j.spark.DataSource") \
  .option("url", "bolt://localhost:7687") \
  .option("authentication.type", "basic") \
  .option("authentication.basic.username", "neo4j") \
  .option("authentication.basic.password", "password") \
  .option("query",
          "MATCH (n:User) WHERE n.screen_name CONTAINS '' RETURN n.screen_name as screen_name,n.id_str as id,n.description as description,n.followers_count as followers_count")\
  .load()

vertex.show()

edges.show()
from graphframes import GraphFrame

g = GraphFrame(vertex,edges)

g.inDegrees.show()

results = g.pageRank(resetProbability=0.01, maxIter=20)
results.vertices.select("id", "pagerank").show()
#   .option("query", "MATCH (n:User) WHERE n.screen_name CONTAINS '' RETURN n.screen_name,n.id_str,n.description,n.followers_count")\
