import pandas as pd
import numpy as np
import json
import io

table_data = pd.read_csv('neo4j-data-analytics/bios.csv')
table_data.fillna("")
table_data.head()

# get 'twitter_id': 'bio_desc'
bio_json = {}
for index, handles in table_data.iterrows():
    bio_json[str(handles['twitter_handle_id'])] = str(handles['twitter_handle_desc'])

# get 'twitter_id': 'twitter_handle'
twitter_handle_json = {}
for index, handles in table_data.iterrows():
    twitter_handle_json[str(handles['twitter_handle_id'])] = str(handles['twitter_handle_desc'])

# neo4j results algo
result_json = json.load(io.open('neo4j-data-analytics/export_algoresults_v2.json', 'r', encoding='utf-8-sig'))

# merge above three datasets in json
for item in result_json:
    user_id = item['n.name']
    item['bio'] = bio_json[user_id]
    item['tweeter_handle'] = twitter_handle_json[user_id]

with open('neo4j-data-analytics/export_algoresults_v3.json', 'w') as fp:
    json.dump(result_json, fp)