
# Returns a variety of information about one or more users specified by the requested IDs.
# https://developer.twitter.com/en/docs/twitter-api/users/lookup/api-reference/get-users
# user look up rate limits - 300 requests per 15 min per app
# up to 100 lookups allowed in a single requests
# i.e. 300*100 = 30,000 IDs lookup per 15 min

# input - folder directory of json files with follower ids
import os
import requests
import pandas as pd
import json
import math
import time


# To set your environment variables in your terminal run the following line:
# export 'BEARER_TOKEN'='<your_bearer_token>'
os.environ['BEARER_TOKEN'] ='AAAAAAAAAAAAAAAAAAAAAJtxMQEAAAAAJZmeOOGETISoJvjAbS1loA3BU0A%3DA3Qdf8LFDm81fyl6rkKd2W1AfHGbkEXYRctvW7zumvsTLmp9nT'



df = pd.read_csv('bq_20210320_users.csv')
df.head()
userList = list(df.twitter_handle_id)
len(userList) #86586 users to get bio
type(userList)
jsonStr = json.dumps(userList)
print(jsonStr)

#modified function to get followers from a Python list instead of json files
def get_followers_from_list(userList):
    var_dict = {}
    print("Estimated time required to get users bio: " + str(math.ceil(len(userList)/30000)*15) + " minutes" )
    #create list of lists of 100 IDs
    sublists = [userList[i:i+100] for i in range(0, len(userList), 100)]
    for i in range(0,len(sublists)):
        user_str = ",".join([str(item) for item in sublists[i]])
        var_dict["string{0}".format(i)] = user_str
    print("Dictionary of keys string0 to string{0} created".format(len(sublists)-1))
    return var_dict




def get_followers(file_directory):
    ### Get followers from json files and create a list of lists (sublist length of 100 IDs)
    ### create dictionary of variable "var_dict" where key = temporary string variables and value = string of 100 IDs
    ### each key:value pair will be one request to Twitter API
    follower_list = []
    # follower_list = "380,244647486"
    var_dict = {}
    # navigate to folder
    os.chdir(file_directory)
    for num in range(1,len(os.listdir())):
        with open('followers{}.json'.format(num)) as json_file:
            data = json.load(json_file)
            for id in data['ids']:
                follower_list.append(id)
    print("{} follower IDs stored in follower_list".format(str(len(follower_list))))
    print("Estimated time required to get followers bio: " + str(math.ceil(len(follower_list)/30000)*15) + " minutes" )
    #create list of lists of 100 IDs
    sublists = [follower_list[i:i+100] for i in range(0, len(follower_list), 100)]
    for i in range(0,len(sublists)):
        follower_str = ",".join([str(item) for item in sublists[i]])
        #follower_str += str(sublists[i], ",")
        #follower_str = follower_str[:-1]
        var_dict["string{0}".format(i)] = follower_str
    print("Dictionary of keys string0 to string{0} created".format(len(sublists)-1))
    return var_dict

#print(len(data['ids'])) # 5000 ids per json file
#print(follower_ids)

def auth():
    return os.environ.get("BEARER_TOKEN")

def create_url(user_id):
    # Replace with user ID below
    user_id = user_id  #elon musk user id 44196397 #michael_saylor user id 244647486 #sgag_sg 606437303
    #kevin systrom 380
    return "https://api.twitter.com/2/users?ids={}".format(user_id)


def get_params():
    ### specify which field to download from Twitter User API
    return{"user.fields":"created_at,description,entities,location,id,name,pinned_tweet_id,"
                          "protected,public_metrics,url,username,verified,withheld",
           "expansions":"pinned_tweet_id"}

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

def get_metrics(json_pyobj):
    for i in range(0, len(json_pyobj['data'])):
        print("retrieving public metrics for user {}".format(json_pyobj["data"][i]["id"]))
        user_id.append(json_pyobj["data"][i]["id"])
        user_name.append(json_pyobj["data"][i]["username"])
        user_desc.append(json_pyobj["data"][i]["description"])
        created_at.append(json_pyobj["data"][i]["created_at"])
        followers_count.append(json_pyobj["data"][i]["public_metrics"]["followers_count"])
        following_count.append(json_pyobj["data"][i]["public_metrics"]["following_count"])
        listed_count.append(json_pyobj["data"][i]["public_metrics"]["listed_count"])
        tweet_count.append(json_pyobj["data"][i]["public_metrics"]["tweet_count"])

#1337*100+25 #133,725 followers
#1338 sublists of 100 follower ids
#1 request = 1 sublist in main()
#var_dict contains dictionary of 1338 follower id strings that should be fed
#1338/300 = 4.46 cycles required

def main(var_dict, i):
        url = create_url(var_dict["string{0}".format(i)])
        headers = create_headers(auth())
        params = get_params()
        json_response = connect_to_endpoint(url, headers, params)
        json_str =  json.dumps(json_response, indent=4, sort_keys=True)
        result = json.loads(json_str) #turn json string into a python dictionary
        all_resp["response{0}".format(i)] = result
        get_metrics(result)

if __name__ == "__main__":
    #file_directory = "D:\\PycharmProjects\\Tweepy_TestRun\\380"  # to update to eventual folder directory
    all_resp = {}
    user_id = []
    user_name = []
    created_at = []
    followers_count = []
    following_count = []
    listed_count = []
    tweet_count = []
    user_desc = []
    json_str = ""
    # modified to get_followers_from_list
    var_dict = get_followers_from_list(userList)
    for i in range(0, len(var_dict)):
        try:
            main(var_dict, i)
        except:
            time.sleep(60*15)
            main(var_dict,i)
        finally:
            print('string{} of users processed! {} requests remaining...'.format(i, len(var_dict)-i-1))

    print("info for", str(len(user_id)) ,"user_ids looked up")

    ### create data frame
    df = pd.DataFrame({"user_id":user_id,
                       "user_name":user_name,
                       "created_at":created_at,
                       "followers_count":followers_count,
                       "following_count":following_count,
                       "listed_count":listed_count,
                       "tweet_count":tweet_count,
                       "user_desc":user_desc})

    df.to_json("users_info.json", orient = "split", compression= "infer")
    print(df.head())

    df.shape #85082,8 (instead of original userList 86586

