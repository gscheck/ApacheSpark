import socket
import sys
import requests
import requests_oauthlib
import json
import time

# Replace the values below with yours
ACCESS_TOKEN = ''
ACCESS_SECRET = ''
CONSUMER_KEY = ''
CONSUMER_SECRET = ''
my_auth = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET,ACCESS_TOKEN, ACCESS_SECRET)

def get_tweets():
    url = 'https://stream.twitter.com/1.1/statuses/filter.json'
    query_data = [('language', 'en'), ('locations', '-130,-20,100,50'),('track','#')]
    query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data])
    # http request to get twitter stream
    response = requests.get(query_url, auth=my_auth, stream=True)
    return response

def send_tweets_to_spark(http_resp, tcp_connection):
    for line in http_resp.iter_lines():
        try:
            print ("--------     Print Tweet     ----------")
            full_tweet = json.loads(line)
            tweet_text = str(full_tweet['text'].encode("utf-8"))
            print(tweet_text)
 
            tweet_text = tweet_text + '\n'
            byt=tweet_text.encode()
            tcp_connection.send(bytes(tweet_text+'\n','utf-8'))
        except:
            e = sys.exc_info()[0]
            print("Error: %s" % e)

TCP_IP = "localhost"
TCP_PORT = 9009
# intialize connection
conn = None

#create socket and listen for a single connection
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)

print("Waiting for TCP connection from Spark Streaming application.")
conn, addr = s.accept()
print("Connected, start getting tweets.")
resp = get_tweets()
send_tweets_to_spark(resp, conn)
