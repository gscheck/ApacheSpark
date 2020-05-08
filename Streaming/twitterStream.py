import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import socket
import json

# Replace the values below with yours
ACCESS_TOKEN = '1242300554006515713-IYCKntEBdSccoPojwJFTXf4Q2AiidN'
ACCESS_SECRET = 'zrMxxaEZPEWDihE1vQE4E1vtpIlMlaqBDL9eySXlCH1sC'
CONSUMER_KEY = 'd2N1KH6ButmAHwq2b3sQ2SXVN'
CONSUMER_SECRET = 'zHNPzGoyDfFcTMpu1BeIdeFvyB4wvPp4p1fQyYkclvoqrN52Ri'

class TweetsListener(StreamListener):
  def __init__(self, csocket):
      self.client_socket = csocket
  def on_data(self, data):
      try:
          msg = json.loads( data )
          self.client_socket.send( msg['text'].encode('utf-8') )
          return True
      except BaseException as e:
          print("Error on_data: %s" % str(e))
      return True
  def on_error(self, status):
      print(status)
      return True
	  
def sendData(c_socket):
  auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
  auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)
  twitter_stream = Stream(auth, TweetsListener(c_socket))
  twitter_stream.filter(track=['guitar'])

TCP_IP = "localhost"
TCP_PORT = 5555
conn = None  
s = socket.socket()
host = "localhost"

s.bind((TCP_IP, TCP_PORT))
s.listen(5)

c, addr = s.accept()
sendData(c)