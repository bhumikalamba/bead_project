
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy import API
from tweepy.streaming import StreamListener
import socket
import json
import pymongo

# ACCESS_TOKEN = 'YOUR_ACCESS_TOKEN'
# ACCESS_SECRET = 'YOUR_ACCESS_SECRET'
# CONSUMER_KEY = 'YOUR_CONSUMER_KEY'
# CONSUMER_SECRET = 'YOUR_CONSUMER_SECRET'

CONSUMER_KEY = "7dJU7wZSHhZwTNXshgczruB7Q"
CONSUMER_SECRET = "tbYF5yp3S0LjNIeV5XMkA33hhuYeuvYOuVVy1X6fgGxJeygWUp"

ACCESS_TOKEN = "313172786-DtYVsYbcWXjDZMGuDIJY3tU8dm1Ax0oXl03RQ4Uz"
ACCESS_SECRET = "aBjwOguGu2bthYjC3xXxSOSbLac1C410B0o4pT9Akfir8"

#
auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN,ACCESS_SECRET)
api = API(auth, wait_on_rate_limit=True,
          wait_on_rate_limit_notify=True)
#api = tweepy.API(auth)

# ## need to configure mongo db path
# class TweetsListener(StreamListener):
#     myclient = pymongo.MongoClient("mongodb://localhost:27017/")
#     mydb = myclient["TwitterDB"]
#     mycol = mydb["tweetsData"]
#
#     def __init__(self, csocket):
#         self.client_socket = csocket
#
#
#     def on_data(self, data):
#         try:
#             msg = json.loads(data)
#             json_data = {
#                 'tweet_id': msg.get('id'),
#                 'twitter_handle_name': msg.get('user').get('screen_name'),
#                 'twitter_handle_id': msg.get('user').get('id'),
#                 'twitter_datetime': msg.get('created_at')
#             }
#             self.mycol.insert_one(json_data)
#             return True
#
#         except BaseException as e:
#             print("Error on_data: %s" % str(e))
#             return True
#
#     def on_error(self, status):
#         print(status)
#         return True

# def getData(c_socket):
#     auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
#     auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)
#     twitter_stream = Stream(auth, TweetsListener(c_socket))
#     twitter_stream.filter(languages='en',track=['bitcoin','btc','blockchain','cryptocurrency','cryptocurrencies','crypto','satoshi'])

class MyStreamListener(tweepy.StreamListener):
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["TwitterDB"]
    mycol = mydb["tweetsData"]

    def on_data(self, data):
        msg = json.loads(data)
        json_data = {
            'tweet_id': msg.get('id'),
            'twitter_handle_name': msg.get('user').get('screen_name'),
            'twitter_handle_id': msg.get('user').get('id'),
            'twitter_datetime': msg.get('created_at')
        }
        self.mycol.insert_one(json_data)
        return True

    # def on_status(self, status):
    #     print(status.text)

    def on_error(self, status_code):
        print(status_code)
        return True

if __name__ == "__main__":
    myStreamListener = MyStreamListener()
    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)

    myStream = Stream(auth=auth, listener=myStreamListener)
    myStream.filter(languages='en',
                          track=['bitcoin', 'btc', 'blockchain', 'cryptocurrency', 'cryptocurrencies', 'crypto',
                                 'satoshi'])


    # s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # Create a socket object
    # host = "localhost"  # Get local machine name
    # port = 5556  # Reserve a port for your service.
    # s.bind((host, port))  # Bind to the port
    #
    #
    #
    # print("Listening on port: %s" % str(port))
    # s.listen(5)  # Now wait for client connection.
    # c, addr = s.accept()  # Establish connection with client.
    # print("Received request from: " + str(addr))
    # print(c)
    # getData(c)

