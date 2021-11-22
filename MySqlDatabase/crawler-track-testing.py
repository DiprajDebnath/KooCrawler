'''
created: 2018-10-16
Info : 
	crawl streaming data and store into json file as a bunch of 10000 tweets in a file
last update:
	2020-04-29
'''
'''
	Adding feature to read user ids from MySql Database
'''
import tweepy, time, json, sys, datetime, traceback, pprint, logging, os, atexit, math
from tweepy import Stream
from collections import OrderedDict 
#import mysql.connector
import socket
# import cPickle as pkl
import pickle
# from keras.models import model_from_json
# import numpy as np
import glob
import os
import re
import string
# reload(sys)
# sys.setdefaultencoding('utf-8')
# import urllib2
# import unicodedata
# import requests
# import pickle

import gc
# import nltk
# from nltk import word_tokenize
# from nltk.util import ngrams
# from nltk.collocations import *
from operator import itemgetter
# from pattern.en import parse
from timeit import default_timer as timer
# import tensorflow as tf


for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)


#Create and configure logger                                                   
logging.basicConfig(filename="new_crawler_track_tweet_10sec.log", format='INFO %(asctime)s %(message)s', filemode='a')
#Creating an object                                                            
logger=logging.getLogger()                                                     
#Setting the threshold of logger to DEBUG                                      
logger.setLevel(logging.INFO) 
#logger.info("5698999")

##initialization..........
tweet_count = 0
datetime1 = datetime.datetime.utcnow()
d1 = str(datetime1).split(".")[0].replace("-", "").replace(":", "").replace(" ", "_")
filename =str(d1)+"-t.json"

final_dict = OrderedDict()




class MyStreamListener(Stream):	
	
	consumer_key = 'nmdxC8Ui1MMZXlJ8OBBEYbaLl'
	consumer_secret = 'hJsbNx1Y8c70b3QxZ2v5dkg44IWWm0AajQ4JpxYPpa4GVgXmDr'
	access_token = '1015642458909298688-govDMmBho8zHzn75iF8X6SKbBUZrUL'
	access_token_secret = '8c7QRpbvCSIBY0KmUExIU50IgHNsGlozVhBRV0c7wVyiO'
	


	#Sets up connection with OAuth
	def __init__(self,csocket):
		self.client_socket = csocket
		super().__init__(self.consumer_key, self.consumer_secret, self.access_token, self.access_token_secret)

	def on_data(self, data):
		print("---------📢📢📢📢📢📢-----------")
		print(type(data))		
		# start_time = time.time()
		full_tweet = json.loads(data)
		# print(type(full_tweet))
		# print (full_tweet)


		try:
			tweet_text = full_tweet['text']	
		except Exception as e:
			logger.info("text is missing------->")
			logger.info(str(e))					
			logger.info(str(datetime.datetime.now()))
			logger.info(data)						
			return
		# print(tweet_text)

		# print(full_tweet)
		data=json.dumps(full_tweet, ensure_ascii=False)
		print(data)
		print(type(data))
		print("____________🚩🚩🚩🚩🚩🚩🚩🚩_____________________")
		# self.client_socket.send(data.encode("utf-8"))
		self.client_socket.send(tweet_text.encode("utf-8")) #Just to stream the tweet text
		# end_time = time.time()
		# print("Time taken is %f",start_time-end_time)
		# write_tweets(data.encode("utf-8"))
		write_tweets(data)


	def on_error(self, status_code):
		self.client_socket.close()
		logger.info("Error in on_error()------->")
		# logger.info(datetime.datetime.now(),status_code)
		if status_code == 420:
			# returning False in on_data disconnects the stream
			logger.info('420')
			return False

	def on_limit(self, status):
		logger.info("Error in on_limit()------->")
		logger.info('Limit threshold exceeded'+str(status))

	def on_timeout(self, status):
		logger.info("Error in on_timeout()------->")
		logger.info('Stream disconnected; continuing...')





class twitterHelper:	
	
	#Sets up connection with OAuth
	def __init__(self,tcp_connection):    
		self.tcp_connection = tcp_connection

	def getStreamTweets(self, topicList):
		try:
			# logger.info(topicList)
			myStream = MyStreamListener(self.tcp_connection)
			myStream.filter(track=topicList)
			# myStream.filter(follow=UserList, async=True)
		except Exception as e:
			logger.info("Error in getStreamTweets()------->")
			logger.info(str(e))




def exit_handler():
	global final_dict, filename, tweets_d
	if len(final_dict) > 50:
		datetime1 = datetime.datetime.utcnow()
		d1 = str(datetime1).split(".")[0].replace("-", "").replace(":", "").replace(" ", "_")
		filename =str(d1)+"-t.json"
		# home_directory = os.environ['HOME']
		# tweets_d = home_directory + "/store/streaming_tweets/track_tweets/"
		# f = open(tweets_d+filename, 'w')
		with open(tweets_d+filename, 'w') as f:
			json.dump(final_dict, f, ensure_ascii=False, indent=4)
		f.close()
		logger.info('My application connection broken')





# Writes all tweets of a user to a file in json format
def write_tweets(twdata):  
	global tweet_count, final_dict, filename, tweets_d, start_time1
	diff_time = math.ceil(timer()-start_time1)
	# print "write tweets1"
	if diff_time>=10:
		# if tweet_count == 1000:	
		# print "write tweets"	
		f = open(tweets_d+filename, 'w')
		json.dump(final_dict, f, ensure_ascii=False, indent=4)
		f.close()		
		##initialization..........
		tweet_count = 0
		datetime1 = datetime.datetime.utcnow()
		d1 = str(datetime1).split(".")[0].replace("-", "").replace(":", "").replace(" ", "_")
		filename =str(d1)+"-t.json"
		start_time1 = timer()
		final_dict = OrderedDict()

	else:
		# bundle_id = str(twdata['id_str'])
		# str11 = bundle_id+", Tweet Count: "+str(tweet_count)
		if diff_time==6:
		# if tweet_count%500==0:
			str11 = "Tweet Count: "+str(tweet_count)
			logger.info(str11)
		# logger.info(tweet_count)
		final_dict.update({tweet_count:twdata})
		tweet_count += 1
		



if __name__ == '__main__':
	tweets_d = "streaming_tweets_json/"

	# print("test")
	TCP_IP = "172.16.117.15"
	TCP_PORT = 9013
	conn = None
	s = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
	s.bind((TCP_IP,TCP_PORT))
	s.listen(1)
	print("Waiting for TCP connection...")
	conn, addr = s.accept()
	print("Connected... Starting getting tweets.")


	t=twitterHelper(conn)
	topicList = []
	#get the trend from twitter................................................................5
	fd = open('trendingTopic_India_twitter.txt','r')
	for line in fd :
		topicList.append(line.strip())
	fd.close()

	xyzList = []
	xyz = open('trackList.txt','r')
	for line in xyz:
		xyzList.append(line.strip())
	xyz.close()

	main_list = topicList+xyzList
	
	logger.info("Streaming Started ---------------------------------- > ")
	start_time1 = timer()
	t.getStreamTweets(main_list)

	atexit.register(exit_handler)