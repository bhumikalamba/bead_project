import datetime

from neomodel import (config, StructuredNode, StringProperty, IntegerProperty,
                      FloatProperty, EmailProperty, Relationship, ZeroOrMore,
                      StructuredRel, AliasProperty, RelationshipTo)  # work with neo4j


config.DATABASE_URL = "bolt://neo4j:password@localhost:7687"

class User(StructuredNode):
    id_str = StringProperty(unique_index=True, required=True)
    screen_name = StringProperty(required=False)
    followers_count = IntegerProperty(required=False)
    friends_count = IntegerProperty(required=False)
    description = StringProperty(required=False)
    follows = RelationshipTo('User', 'FOLLOWS')