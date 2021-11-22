import tweepy
from tweepy import Stream
import socket
import json

import os
from dotenv import load_dotenv
load_dotenv() # take environment variables from .env

consumer_key = os.getenv("consumer_key")
consumer_secret = os.getenv("consumer_secret")
access_token = os.getenv("access_token")
access_token_secret = os.getenv("access_token_secret")

class TweetsListener(Stream):
    def __init__(self, consumer_key, consumer_secret, access_token, access_token_secret, csocket):
        self.client_socket = csocket
        super().__init__(consumer_key, consumer_secret, access_token, access_token_secret)
        
    def on_data(self, raw_data):
        try:
            msg = json.loads(raw_data)
            print(msg['text'].encode('utf-8'))
            print(msg['created_at'].encode('utf-8'))
            
            self.client_socket.send(msg['text'].encode('utf-8'))
            return True
        except Exception as e:
            print("Error on_data: %s" % str(e))
        return True
    
    def on_status(self, status):
        print(status.text)

def sendData(c_socket): # c_socket or client socket
    twitter_stream = TweetsListener(consumer_key, consumer_secret, access_token, access_token_secret, c_socket)
    twitter_stream.filter(track=['BJP'])
    
if __name__ == "__main__":
    s = socket.socket()     # Create a socket object
    host = '172.16.117.15'  # Get local machine name
    port = 9019             # Reserce a port for your service
    s.bind((host, port))    # Bind to the port
    print("Listening on port: %s" % str(port))
          
    s.listen(5)            # Now wait for client connection.
    c, addr = s.accept()   # Establish connection with client.
    print("Recived request from: " + str(addr))
          
    sendData(c)
