from neomodel import (config, StructuredNode, StringProperty, IntegerProperty,RelationshipTo)  # work with neo4j


config.DATABASE_URL = "bolt://username:password@Ip-add:7687"


## create schema on neo4j
class User(StructuredNode):
    id_str = StringProperty(unique_index=True, required=True)
    screen_name = StringProperty(required=False)
    followers_count = IntegerProperty(required=False)
    friends_count = IntegerProperty(required=False)
    description = StringProperty(required=False)
    follows = RelationshipTo('User', 'FOLLOWS')

