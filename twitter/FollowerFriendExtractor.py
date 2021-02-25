import tweepy
import time

CONSUMER_KEY = "7dJU7wZSHhZwTNXshgczruB7Q"
CONSUMER_TOKEN = "tbYF5yp3S0LjNIeV5XMkA33hhuYeuvYOuVVy1X6fgGxJeygWUp"

ACCESS_TOKEN = "313172786-DtYVsYbcWXjDZMGuDIJY3tU8dm1Ax0oXl03RQ4Uz"
ACCESS_TOKEN_SECRET = "aBjwOguGu2bthYjC3xXxSOSbLac1C410B0o4pT9Akfir8"

auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_TOKEN)
auth.set_access_token(ACCESS_TOKEN,ACCESS_TOKEN_SECRET)

api = tweepy.API(auth)

## get the follower details
## identified screen_names should be replaced and loop over.
## May need to re-adjust time.sleep() with this if len(page) == 5000: time.sleep(60)
## adopted from https://stackoverflow.com/questions/17431807/get-all-follower-ids-in-twitter-by-tweepy
## allowed_param: 'id', 'user_id', 'screen_name', 'cursor', 'count'. In other words can get the data  via these fields.
## api.followers -> returns the entire user objects. will be heavy.

follower_ids = []

twitter_handles = ['Elon','Bezon']
for page in tweepy.Cursor(api.followers_ids, screen_name="surendhar7").pages():
    follower_ids.extend(page)
    time.sleep(60)
print (follower_ids)
#
# friend_ids = []
# for page in tweepy.Cursor(api.friends_ids, screen_name="surendhar7").pages():
#     friend_ids.extend(page)
#     time.sleep(60)

tweets = []
for page in tweepy.Cursor(api.search,q='#bitcoin').pages():
    tweets.extend(page)
    time.sleep(60)

print(tweets)
print(len(tweets))

# print(friend_ids)

## Need to confirm the data we need to parse for neo4j to do centrality algo/community detection




