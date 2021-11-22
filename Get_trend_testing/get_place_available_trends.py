# -*- coding: utf-8 -*-

'''
updated - 29/06/2018
update info - start trending 
'''
'''
updated - 11/11/2021
update info - upgrading code to python3
author - dipraj@rnd.iit.ac.in
'''


import traceback, logging
import tweepy, datetime
# from cassandra.cluster import Cluster
import unicodedata
# from cassandra import ConsistencyLevel
# from cassandra.auth import PlainTextAuthProvider
# from cassandra.policies import RoundRobinPolicy
import sys
# reload(sys)
# sys.setdefaultencoding('utf-8')

#Create and configure logger                                                   
logging.basicConfig(filename="get_place_available_trends.log", format='INFO %(asctime)s %(message)s', filemode='a')
#Creating an object                                                            
logger=logging.getLogger()                                                     
#Setting the threshold of logger to DEBUG                                      
logger.setLevel(logging.INFO) 
#logger.info("5698999")


# Gets top 50 current trends of the place specified by WEOID
def getCurrentTrends(api, place):
	# India(WEOID)-23424848
	# return api.trends_place(place)
	return api.get_place_trends(place)
	
	
def get_homeTimeline(api, place):
	return api.home_timeline()
	
def get_available_trends(api, place):
	return api.trends_available() #tweepy function
	





if __name__ == '__main__':
	consumer_key = "l8iRpy20itfbanmiZNaM4Duvr"
	consumer_secret = "8pUeXtYnHiJT2GJza1R2JFm8zvhgMOdggyNmhR3nBDn524lJww"
	# access_token = "1015642458909298688-Am7JsA18DlAqfUEfjSxeWFoexjJzta"
	# access_token_secret = "JXUGhLstHGuZD9Pfb9kT3VMYg5fFtOL3sC2Tfh6osArjQ"


	auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
	# auth.set_access_token(access_token, access_token_secret)
	api = tweepy.API(auth)

    

	
	try:
		logger.info("start crawl trending from india")
		trending_list1 = []
		trends_list = getCurrentTrends(api, '23424848')
		print("################################")
		print(trends_list)
		print("################################")

		flag = False

		fd = open('trendingTopic_India_twitter.txt','w')	
		for item in trends_list:
			for i in range(len(item['trends'])):
				# print >> fd , item['trends'][i]['name']	
				print (fd , item['trends'][i]['name'])	
				fd.write(item['trends'][i]['name'] + "\n") #Line added to write into file
						
				key = str(item['trends'][i]['name'])
				value = str(item['trends'][i]['tweet_volume'])
				t1 = {key: value}
				trending_list1.append((t1))	
			if not flag:
				d1 = item['created_at']
				if isinstance(d1, datetime.datetime):
					date1 = d1.date()
				else:
					d1 = datetime.datetime.strptime(d1, "%Y-%m-%dT%H:%M:%SZ")
					date1 = d1.date()
				loc = item['locations'][0]['name']
		print (trending_list1)
		fd.close()
		
		logger.info("completed")
	except Exception as e:		
		logger.info("394878")
		logger.info(str(e))
		sys.exit()


	############
	##New Finding Below Portion is Not Used##
	############
	
	# try:
	# 	logger.info("start crawl trending from delhi")
	# 	trending_list1_delhi = []
	# 	trends_list = getCurrentTrends(api, '20070458')
	# 	for item in trends_list:
	# 		for i in range(len(item['trends'])):				
	# 			key = str(item['trends'][i]['name'])
	# 			value = str(item['trends'][i]['tweet_volume'])
	# 			t1 = {key: value}
	# 			trending_list1_delhi.append((t1))	
	# 		if not flag:
	# 			d1 = item['created_at']
	# 			if isinstance(d1, datetime.datetime):
	# 				date1 = d1.date()
	# 			else:
	# 				d1 = datetime.datetime.strptime(d1, "%Y-%m-%dT%H:%M:%SZ")
	# 				date1 = d1.date()
	# 				loc = item['locations'][0]['name']
	# 	print (trending_list1_delhi)
	# 	logger.info("completed")
	# except Exception as e:
	# 	logger.info("3111")                                                    
	# 	logger.info(str(e))                                                      
	# 	sys.exit()

	# try:
	# 	logger.info("start crawl trending from world")
	# 	trending_list1_world = []
	# 	trends_list = getCurrentTrends(api, '1')
	# 	for item in trends_list:
	# 		for i in range(len(item['trends'])):				
	# 			key = str(item['trends'][i]['name'])
	# 			value = str(item['trends'][i]['tweet_volume'])
	# 			t1 = {key: value}
	# 			trending_list1_world.append((t1))	
	# 		if not flag:
	# 			d1 = item['created_at']
	# 			if isinstance(d1, datetime.datetime):
	# 				date1 = d1.date()
	# 			else:
	# 				d1 = datetime.datetime.strptime(d1, "%Y-%m-%dT%H:%M:%SZ")
	# 				date1 = d1.date()
	# 				loc = item['locations'][0]['name']
	# 	print (trending_list1_world)
	# 	logger.info("completed")
	# except Exception as e:
	# 	logger.info("3222")                                                    
	# 	logger.info(str(e))                                                      
	# 	sys.exit()

	

