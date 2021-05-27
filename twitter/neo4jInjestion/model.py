from neomodel import (config, StructuredNode, StringProperty, IntegerProperty,RelationshipTo)  # work with neo4j


config.DATABASE_URL = "bolt://neo4j:$martBEAD&@34.87.46.194:7687"
#db.set_connection("bolt://neo4j:$martBEAD&@34.87.46.194:7474")

## create schema on neo4j
class User(StructuredNode):
    id_str = StringProperty(unique_index=True, required=True)
    screen_name = StringProperty(required=False)
    followers_count = IntegerProperty(required=False)
    friends_count = IntegerProperty(required=False)
    description = StringProperty(required=False)
    follows = RelationshipTo('User', 'FOLLOWS')

