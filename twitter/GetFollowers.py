# Fetching the followers list
# initial code from https://github.com/twitterdev/Twitter-API-v2-sample-code/blob/master/Follows-Lookup/followers_lookup.py

# notes on rate limits: https://stackoverflow.com/questions/58542763/twitter-api-efficient-way-to-get-followers-lists-for-accounts-with-few-million

# reference for get_params()
# https://developer.twitter.com/en/docs/twitter-api/v1/accounts-and-users/follow-search-get-users/api-reference/get-followers-ids


import requests
import os
import json
import time
import shutil

# To set your environment variables in your terminal run the following line:
# export 'BEARER_TOKEN'='<your_bearer_token>'
os.environ['BEARER_TOKEN'] ='AAAAAAAAAAAAAAAAAAAAAJtxMQEAAAAAJZmeOOGETISoJvjAbS1loA3BU0A%3DA3Qdf8LFDm81fyl6rkKd2W1AfHGbkEXYRctvW7zumvsTLmp9nT'

def auth():
    return os.environ.get("BEARER_TOKEN")

# example to get first page of followers from one user id
    #def create_url():
    #    # Replace with user ID below
    #    user_id = 44196397  #elon musk user id 44196397
    #    return "https://api.twitter.com/2/users/{}/followers".format(user_id)

    #def get_params():
    #    return {"user.fields": "created_at"}

def create_url(user_id):
    # Replace with user ID below
    user_id = user_id  #elon musk user id 44196397 #michael_saylor user id 244647486 #sgag_sg 606437303
    #kevin systrom 380
    return "https://api.twitter.com/1.1/followers/ids.json?user_id={}".format(user_id)

def get_params(nextcursor):
    return{"count":5000, "cursor":nextcursor}
# default cursor =  -1, refering to first page of results
# example https://api.twitter.com/1.1/followers/ids.json?cursor=-1&screen_name=andypiper&count=5000

def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers


def connect_to_endpoint(url, headers, params):
    response = requests.request("GET", url, headers=headers, params=params)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(
            "Request returned an error: {} {}".format(
                response.status_code, response.text
            )
        )
    return response.json()

def export_data_to_json(data, filename):
    # save to json file
    with open(filename, 'w') as fp:
        json.dump(data, fp)


def load_data_from_json(filename):
    with open(filename, 'r') as fp:
        data = json.load(fp)
    return data

def export_else_append(data, filename):
    if os.path.isfile(filename):
        print("File exist & appended")
        apppend_data_to_json(data, filename)
    else:
        print("File not exist. New File created.")
        export_data_to_json(data, filename)


def main():
    bearer_token = auth()
    url = create_url(user_id)
    headers = create_headers(bearer_token)
    params = get_params(nextcursor)
    json_response = connect_to_endpoint(url, headers, params)
    #print(json.dumps(json_response, indent=4, sort_keys=True)
    export_data_to_json(json_response, "followers{}.json".format(currfilecount))


if __name__ == "__main__":
    user_list = [380, 244647486]
    for user in user_list:
        # create folder
        os.makedirs(str(user))
        user_id = user
        currfilecount = 1
        nextcursor = -1 #first results page
        try:
            main()
        except:
            time.sleep(60 * 15)
            main()
        finally:
            print('File1 for user_id {} eExported. Loading File1...'.format(user_id))

        data = load_data_from_json("followers{}.json".format(currfilecount))
        print('Getting subsequent files...')
        while nextcursor != 0:
            data = load_data_from_json("followers{}.json".format(currfilecount))
            nextcursor = data['next_cursor']
            currfilecount += 1
            try:
                main()
            except:
                time.sleep(60*15)
                main()
            finally:
                print("File followers{} exported!".format(currfilecount))
        print("Extraction completed for user_id {}... Moving files".format(user_id))
        files = os.listdir()
        dest = os.getcwd()+"\\{}".format(user)
        for f in files:
            if(f.startswith("followers")):
                shutil.move(f,dest)
        print("files moves into user_id {} folder".format(user_id))


#time.sleep(60*15)
#print('times up!')
# rate limit 15 request in 15 minutes, 5000 each requests

#Traceback (most recent call last):
#  File "<input>", line 12, in <module>
#  File "<input>", line 74, in main
#  File "<input>", line 44, in connect_to_endpoint
#Exception: Request returned an error: 429 {"errors":[{"message":"Rate limit exceeded","code":88}]}


###### TEMP ###########
nextcursor
data = load_data_from_json("followers1.json")
print(data['next_cursor'])
print(data['previous_cursor'])
len(data['ids'])
print(data['ids'])

#sgag_sg 587.9k followers

#michaek saylor 569,042 will take 7.59 cycles x 15 minutes = 120 minutes (2 hrs)
#kevin systrom 133,729 followers - will take 1.78 x 15 minutes
#followers1 - next cursor = 1674029966258931775, previous_cursor = 0
#followers2 - next cursor = 1664840710012508777 , previous cursor = -1674029888770414982

