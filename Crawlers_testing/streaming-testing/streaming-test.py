"""
   DISCLAIMER:

	spark_operation.py:
	This file was created by Rahul Yumlembam and Anurag Kushwaha on 23-10-2019 
	in OSiNT Lab, IIT Guwahati as a part of the Tweet-Analytics project.

   LICENSE:

	All rights reserved. No part of this file maybe
	reproduced or used in any manner whatsoever without the 
	express written permission of the owner.
	Refer to the file "LICENSE" for full license details.

"""
"""

Check List:

- [X] Naming conventions 
	
- [X] Comments 

- [X] Resource leakage

- [] Test cases 

- [] Log files

eg - [X] is a completed checkbox

"""

"""

Introduction(what is function does)

:param param1: this is a first param
:param param2: this is a second param
:returns: this is a description of what is returned
:raises keyError: raises an exception

"""


from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
from pyspark.sql.functions import explode
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.functions import lit,unix_timestamp
from pyspark.sql import functions as F
from pyspark.sql.types import *
import requests
import cPickle as pkl
import pickle
import logging
import json
from os import system, name 
from time import sleep 
import io
import re
import datetime
from itertools import product
import sys
sys.path.append('..')
sys.path.insert(1,r'../../python_script/tweet_parser/')
reload(sys)
sys.setdefaultencoding("utf-8")
from tweet_spark import Tweet
from timeit import default_timer as timer
sys.path.insert(1,r'../batch/')
from load_store import batch
from spark_operation import operation
from pyspark.sql.functions import date_format
import configparser
from datetime import timedelta

# intitialse config parser
config = configparser.ConfigParser() 
# read the config file ,the path should be mounted in all the worker
config.read(r'../config/sparkconfig.txt') 
# printing out the section for debugging propose
print(config.sections()) 
# #setting the values from config file
 # get the IP
IP=config['sparkbatch']['IP']
# get the username
USERNAME=config['sparkbatch']['USERNAME'] 
# get the password
PASSWORD=config['sparkbatch']['PASSWORD'] 
 # get the kyro value
KYROVALUE=config['sparkbatch']['KYROVALUE']
# get the spark_local dir
SPARK_LOCAL_DIR=config['sparkbatch']['SPARK_LOCAL_DIR']

SPARK_HOSTNAME=config['sparkbatch']['SPARK_HOSTNAME'] 
SPARK_PORT1=config['sparkbatch']['SPARK_PORT1'] #9012 - location
SPARK_PORT2=config['sparkbatch']['SPARK_PORT2'] #9013 - track
SPARK_PORT3=config['sparkbatch']['SPARK_PORT3'] #9014 - handle
BATCHSIZE=config['sparkbatch']['BATCHSIZE'] 
BLOCKINTERVAL = config['sparkbatch']['BLOCKINTERVAL']
CONCURRENTJOBS = config['sparkbatch']['CONCURENTJOBS'] 
SERIALIZER=config['sparkbatch']['SPARK_SERIALIZER'] 
DRIVER_EXTRA_JAVA=config['sparkbatch']['SPARK_DRIVER_EXTRA_JAVA_OPTIONS'] 
EXECUTOR_EXTRA_JAVA=config['sparkbatch']['SPARK_EXECUTOR_EXTRA_JAVA_OPTIONS'] 
STREAMING_MAX_RATE=config['sparkbatch']['SPARK_STREAMING_RECIEVER_MAX_RATE'] 
KEEP_ALIVE=config['sparkbatch']['KEEP_ALIVE']
INPUT_CONSISTENCY=config['sparkbatch']['INPUT_CONSISTENCY']
OUTPUT_CONSISTENCY=config['sparkbatch']['OUTPUT_CONSISTENCY']
CONCURRENT_WRITES=config['sparkbatch']['CONCURRENT_WRITES']
GROUPING_KEY=config['sparkbatch']['GROUPING_KEY']
ROW_SIZE=config['sparkbatch']['ROW_SIZE']
CLEANER_TTL=config['sparkbatch']['CLEANER_TTL']
GC_INTERVAL=config['sparkbatch']['GC_INTERVAL']
REFERENCE_TRACKING_BLOCKING=config['sparkbatch']['REFERENCE_TRACKING_BLOCKING']
UI_RETAINED_EXECUTOR=config['sparkbatch']['UI_RETAINED_EXECUTOR']
UI_RETAINED_DRIVER=config['sparkbatch']['UI_RETAINED_DRIVER']
SQL_UI_RETAINED_EXECUTOR=config['sparkbatch']['SQL_UI_RETAINED_EXECUTOR']
STREAMING_UI_RETAINED_BATCHES=config['sparkbatch']['STREAMING_UI_RETAINED_BATCHES']
UI_RETAINED_JOBS=config['sparkbatch']['UI_RETAINED_JOBS']
UI_RETAINED_STAGES=config['sparkbatch']['UI_RETAINED_STAGES']
UI_RETAINED_TASKS=config['sparkbatch']['UI_RETAINED_TASKS']
STREAMING_BACK_PRESSURE_ENABLE=config['sparkbatch']['STREAMING_BACK_PRESSURE_ENABLE']
# create batch object using load_store.py
batchData = batch(IP,USERNAME,PASSWORD,KYROVALUE,"no_conf",BLOCKINTERVAL,CONCURRENTJOBS,DRIVER_EXTRA_JAVA,\
			EXECUTOR_EXTRA_JAVA,STREAMING_MAX_RATE,"Streaming",KEEP_ALIVE,INPUT_CONSISTENCY,OUTPUT_CONSISTENCY,\
			CONCURRENT_WRITES,GROUPING_KEY,ROW_SIZE,CLEANER_TTL,GC_INTERVAL,REFERENCE_TRACKING_BLOCKING,\
			UI_RETAINED_EXECUTOR,UI_RETAINED_DRIVER,SQL_UI_RETAINED_EXECUTOR,\
			STREAMING_UI_RETAINED_BATCHES,UI_RETAINED_JOBS,UI_RETAINED_STAGES,UI_RETAINED_TASKS,STREAMING_BACK_PRESSURE_ENABLE,SERIALIZER)



# table names to store processed data (10sec)
TOKEN_COUNT_TABLE = config['streaming']['TOKEN_COUNT']
TOKEN_CO_OCCUR_TABLE = config['streaming']['TOKEN_CO_OCCUR']
TOKEN_LOCATION_TABLE = config['streaming']['TOKEN_LOCATION']



# create batch operation object using spark_operation.py
batchDataOperation = operation() 
#get spark streaming context
ssc = batchData.get_streaming_context(int(BATCHSIZE))
#recieving data from 
data_stream1 = ssc.socketTextStream(SPARK_HOSTNAME,int(SPARK_PORT1))
#recieving data from
data_stream2 = ssc.socketTextStream(SPARK_HOSTNAME,int(SPARK_PORT2))
# #recieving data from
data_stream3 = ssc.socketTextStream(SPARK_HOSTNAME,int(SPARK_PORT3))

#creating tweet parser object with config
tweet_object =  Tweet("../../python_script/config/config.txt")

#sensitivity
fd = open('../files/dictionary.txt','r')
topicList = []
for line in fd :
	topicList.append(line.strip())
fd.close()

#T_E_Analysis
te = open('../files/list_of_words_event.txt','r')
te_list = []
for line in te :
	te_list.append(line.strip())
te.close()

#--------------------------Location--------------------------------------------

try:
	import _geohash
except ImportError:
	_geohash = None

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

__all__ = ['encode','decode','decode_exactly','bbox', 'neighbors', 'expand']

_base32 = '0123456789bcdefghjkmnpqrstuvwxyz'
_base32_map = {}
for i in range(len(_base32)):
	_base32_map[_base32[i]] = i
del i

def _encode_i2c(lat,lon,lat_length,lon_length):
	precision = int((lat_length+lon_length)/5)
	if lat_length < lon_length:
		a = lon
		b = lat
	else:
		a = lat
		b = lon
	
	boost = (0,1,4,5,16,17,20,21)
	ret = ''
	for i in range(precision):
		ret+=_base32[(boost[a&7]+(boost[b&3]<<1))&0x1F]
		t = a>>3
		a = b>>2
		b = t
	
	return ret[::-1]

def encode(latitude, longitude, precision=12):
	if latitude >= 90.0 or latitude < -90.0:
		raise Exception("invalid latitude.")
	while longitude < -180.0:
		longitude += 360.0
	while longitude >= 180.0:
		longitude -= 360.0
	
	if _geohash:
		basecode=_geohash.encode(latitude,longitude)
		if len(basecode)>precision:
			return basecode[0:precision]
		return basecode+'0'*(precision-len(basecode))
	
	lat = latitude/180.0
	lon = longitude/360.0
	
	xprecision=precision+1
	lat_length = lon_length = int(xprecision*5/2)
	if xprecision%2==1:
		lon_length+=1
	
	if lat>0:
		lat = int((1<<lat_length)*lat)+(1<<(lat_length-1))
	else:
		lat = (1<<lat_length-1)-int((1<<lat_length)*(-lat))
	
	if lon>0:
		lon = int((1<<lon_length)*lon)+(1<<(lon_length-1))
	else:
		lon = (1<<lon_length-1)-int((1<<lon_length)*(-lon))
	
	return _encode_i2c(lat,lon,lat_length,lon_length)[:precision]

def _decode_c2i(hashcode):
	lon = 0
	lat = 0
	bit_length = 0
	lat_length = 0
	lon_length = 0
	for i in hashcode:
		t = _base32_map[i]
		if bit_length%2==0:
			lon = lon<<3
			lat = lat<<2
			lon += (t>>2)&4
			lat += (t>>2)&2
			lon += (t>>1)&2
			lat += (t>>1)&1
			lon += t&1
			lon_length+=3
			lat_length+=2
		else:
			lon = lon<<2
			lat = lat<<3
			lat += (t>>2)&4
			lon += (t>>2)&2
			lat += (t>>1)&2
			lon += (t>>1)&1
			lat += t&1
			lon_length+=2
			lat_length+=3
		
		bit_length+=5
	
	return (lat,lon,lat_length,lon_length)

def decode(hashcode, delta=False):
	'''
	decode a hashcode and get center coordinate, and distance between center and outer border
	'''
	if _geohash:
		(lat,lon,lat_bits,lon_bits) = _geohash.decode(hashcode)
		latitude_delta = 90.0/(1<<lat_bits)
		longitude_delta = 180.0/(1<<lon_bits)
		latitude = lat + latitude_delta
		longitude = lon + longitude_delta
		if delta:
			return latitude,longitude,latitude_delta,longitude_delta
		return latitude,longitude
	
	(lat,lon,lat_length,lon_length) = _decode_c2i(hashcode)
	
	lat = (lat<<1) + 1
	lon = (lon<<1) + 1
	lat_length += 1
	lon_length += 1
	
	latitude  = 180.0*(lat-(1<<(lat_length-1)))/(1<<lat_length)
	longitude = 360.0*(lon-(1<<(lon_length-1)))/(1<<lon_length)
	if delta:
		latitude_delta  = 180.0/(1<<lat_length)
		longitude_delta = 360.0/(1<<lon_length)
		return latitude,longitude,latitude_delta,longitude_delta
	
	return latitude,longitude

def decode_exactly(hashcode):
	return decode(hashcode, True)

## hashcode operations below

def bbox(hashcode):
	'''
	decode a hashcode and get north, south, east and west border.
	'''
	if _geohash:
		(lat,lon,lat_bits,lon_bits) = _geohash.decode(hashcode)
		latitude_delta = 180.0/(1<<lat_bits)
		longitude_delta = 360.0/(1<<lon_bits)
		return {'s':lat,'w':lon,'n':lat+latitude_delta,'e':lon+longitude_delta}
	
	(lat,lon,lat_length,lon_length) = _decode_c2i(hashcode)
	ret={}
	if lat_length:
		ret['n'] = 180.0*(lat+1-(1<<(lat_length-1)))/(1<<lat_length)
		ret['s'] = 180.0*(lat-(1<<(lat_length-1)))/(1<<lat_length)
	else: # can't calculate the half with bit shifts (negative shift)
		ret['n'] = 90.0
		ret['s'] = -90.0
	
	if lon_length:
		ret['e'] = 360.0*(lon+1-(1<<(lon_length-1)))/(1<<lon_length)
		ret['w'] = 360.0*(lon-(1<<(lon_length-1)))/(1<<lon_length)
	else: # can't calculate the half with bit shifts (negative shift)
		ret['e'] = 180.0
		ret['w'] = -180.0
	
	return ret

def neighbors(hashcode):
	if _geohash and len(hashcode)<25:
		return _geohash.neighbors(hashcode)
	(lat,lon,lat_length,lon_length) = _decode_c2i(hashcode)
	ret = []
	tlat = lat
	for tlon in (lon-1, lon+1):
		code = _encode_i2c(tlat,tlon,lat_length,lon_length)
		if code:
			ret.append(code)
	
	tlat = lat+1
	if not tlat >> lat_length:
		for tlon in (lon-1, lon, lon+1):
			ret.append(_encode_i2c(tlat,tlon,lat_length,lon_length))
	
	tlat = lat-1
	if tlat >= 0:
		for tlon in (lon-1, lon, lon+1):
			ret.append(_encode_i2c(tlat,tlon,lat_length,lon_length))
	
	return ret

def expand(hashcode):
	ret = neighbors(hashcode)
	ret.append(hashcode)
	return ret

def _uint64_interleave(lat32, lon32):
	intr = 0
	boost = (0,1,4,5,16,17,20,21,64,65,68,69,80,81,84,85)
	for i in range(8):
		intr = (intr<<8) + (boost[(lon32>>(28-i*4))%16]<<1) + boost[(lat32>>(28-i*4))%16]
	
	return intr

def _uint64_deinterleave(ui64):
	lat = lon = 0
	boost = ((0,0),(0,1),(1,0),(1,1),(0,2),(0,3),(1,2),(1,3),
			 (2,0),(2,1),(3,0),(3,1),(2,2),(2,3),(3,2),(3,3))
	for i in range(16):
		p = boost[(ui64>>(60-i*4))%16]
		lon = (lon<<2) + p[0]
		lat = (lat<<2) + p[1]
	
	return (lat, lon)

def encode_uint64(latitude, longitude):
	if latitude >= 90.0 or latitude < -90.0:
		raise ValueError("Latitude must be in the range of (-90.0, 90.0)")
	while longitude < -180.0:
		longitude += 360.0
	while longitude >= 180.0:
		longitude -= 360.0
	
	if _geohash:
		ui128 = _geohash.encode_int(latitude,longitude)
		if _geohash.intunit == 64:
			return ui128[0]
		elif _geohash.intunit == 32:
			return (ui128[0]<<32) + ui128[1]
		elif _geohash.intunit == 16:
			return (ui128[0]<<48) + (ui128[1]<<32) + (ui128[2]<<16) + ui128[3]
	
	lat = int(((latitude + 90.0)/180.0)*(1<<32))
	lon = int(((longitude+180.0)/360.0)*(1<<32))
	return _uint64_interleave(lat, lon)

def decode_uint64(ui64):
	if _geohash:
		latlon = _geohash.decode_int(ui64 % 0xFFFFFFFFFFFFFFFF)
		if latlon:
			return latlon
	
	lat,lon = _uint64_deinterleave(ui64)
	return (180.0*lat/(1<<32) - 90.0, 360.0*lon/(1<<32) - 180.0)

def expand_uint64(ui64, precision=50):
	ui64 = ui64 & (0xFFFFFFFFFFFFFFFF << (64-precision))
	lat,lon = _uint64_deinterleave(ui64)
	lat_grid = 1<<(32-int(precision/2))
	lon_grid = lat_grid>>(precision%2)
	
	if precision<=2: # expand becomes to the whole range
		return []
	
	ranges = []
	if lat & lat_grid:
		if lon & lon_grid:
			ui64 = _uint64_interleave(lat-lat_grid, lon-lon_grid)
			ranges.append((ui64, ui64 + (1<<(64-precision+2))))
			if precision%2==0:
				# lat,lon = (1, 1) and even precision
				ui64 = _uint64_interleave(lat-lat_grid, lon+lon_grid)
				ranges.append((ui64, ui64 + (1<<(64-precision+1))))
				
				if lat + lat_grid < 0xFFFFFFFF:
					ui64 = _uint64_interleave(lat+lat_grid, lon-lon_grid)
					ranges.append((ui64, ui64 + (1<<(64-precision))))
					ui64 = _uint64_interleave(lat+lat_grid, lon)
					ranges.append((ui64, ui64 + (1<<(64-precision))))
					ui64 = _uint64_interleave(lat+lat_grid, lon+lon_grid)
					ranges.append((ui64, ui64 + (1<<(64-precision))))
			else:
				# lat,lon = (1, 1) and odd precision
				if lat + lat_grid < 0xFFFFFFFF:
					ui64 = _uint64_interleave(lat+lat_grid, lon-lon_grid)
					ranges.append((ui64, ui64 + (1<<(64-precision+1))))
					
					ui64 = _uint64_interleave(lat+lat_grid, lon+lon_grid)
					ranges.append((ui64, ui64 + (1<<(64-precision))))
				
				ui64 = _uint64_interleave(lat, lon+lon_grid)
				ranges.append((ui64, ui64 + (1<<(64-precision))))
				ui64 = _uint64_interleave(lat-lat_grid, lon+lon_grid)
				ranges.append((ui64, ui64 + (1<<(64-precision))))
		else:
			ui64 = _uint64_interleave(lat-lat_grid, lon)
			ranges.append((ui64, ui64 + (1<<(64-precision+2))))
			if precision%2==0:
				# lat,lon = (1, 0) and odd precision
				ui64 = _uint64_interleave(lat-lat_grid, lon-lon_grid)
				ranges.append((ui64, ui64 + (1<<(64-precision+1))))
				
				if lat + lat_grid < 0xFFFFFFFF:
					ui64 = _uint64_interleave(lat+lat_grid, lon-lon_grid)
					ranges.append((ui64, ui64 + (1<<(64-precision))))
					ui64 = _uint64_interleave(lat+lat_grid, lon)
					ranges.append((ui64, ui64 + (1<<(64-precision))))
					ui64 = _uint64_interleave(lat+lat_grid, lon+lon_grid)
					ranges.append((ui64, ui64 + (1<<(64-precision))))
			else:
				# lat,lon = (1, 0) and odd precision
				if lat + lat_grid < 0xFFFFFFFF:
					ui64 = _uint64_interleave(lat+lat_grid, lon)
					ranges.append((ui64, ui64 + (1<<(64-precision+1))))
					
					ui64 = _uint64_interleave(lat+lat_grid, lon-lon_grid)
					ranges.append((ui64, ui64 + (1<<(64-precision))))
				ui64 = _uint64_interleave(lat, lon-lon_grid)
				ranges.append((ui64, ui64 + (1<<(64-precision))))
				ui64 = _uint64_interleave(lat-lat_grid, lon-lon_grid)
				ranges.append((ui64, ui64 + (1<<(64-precision))))
	else:
		if lon & lon_grid:
			ui64 = _uint64_interleave(lat, lon-lon_grid)
			ranges.append((ui64, ui64 + (1<<(64-precision+2))))
			if precision%2==0:
				# lat,lon = (0, 1) and even precision
				ui64 = _uint64_interleave(lat, lon+lon_grid)
				ranges.append((ui64, ui64 + (1<<(64-precision+1))))
				
				if lat > 0:
					ui64 = _uint64_interleave(lat-lat_grid, lon-lon_grid)
					ranges.append((ui64, ui64 + (1<<(64-precision))))
					ui64 = _uint64_interleave(lat-lat_grid, lon)
					ranges.append((ui64, ui64 + (1<<(64-precision))))
					ui64 = _uint64_interleave(lat-lat_grid, lon+lon_grid)
					ranges.append((ui64, ui64 + (1<<(64-precision))))
			else:
				# lat,lon = (0, 1) and odd precision
				if lat > 0:
					ui64 = _uint64_interleave(lat-lat_grid, lon-lon_grid)
					ranges.append((ui64, ui64 + (1<<(64-precision+1))))
					
					ui64 = _uint64_interleave(lat-lat_grid, lon+lon_grid)
					ranges.append((ui64, ui64 + (1<<(64-precision))))
				ui64 = _uint64_interleave(lat, lon+lon_grid)
				ranges.append((ui64, ui64 + (1<<(64-precision))))
				ui64 = _uint64_interleave(lat+lat_grid, lon+lon_grid)
				ranges.append((ui64, ui64 + (1<<(64-precision))))
		else:
			ui64 = _uint64_interleave(lat, lon)
			ranges.append((ui64, ui64 + (1<<(64-precision+2))))
			if precision%2==0:
				# lat,lon = (0, 0) and even precision
				ui64 = _uint64_interleave(lat, lon-lon_grid)
				ranges.append((ui64, ui64 + (1<<(64-precision+1))))
				
				if lat > 0:
					ui64 = _uint64_interleave(lat-lat_grid, lon-lon_grid)
					ranges.append((ui64, ui64 + (1<<(64-precision))))
					ui64 = _uint64_interleave(lat-lat_grid, lon)
					ranges.append((ui64, ui64 + (1<<(64-precision))))
					ui64 = _uint64_interleave(lat-lat_grid, lon+lon_grid)
					ranges.append((ui64, ui64 + (1<<(64-precision))))
			else:
				# lat,lon = (0, 0) and odd precision
				if lat > 0:
					ui64 = _uint64_interleave(lat-lat_grid, lon)
					ranges.append((ui64, ui64 + (1<<(64-precision+1))))
					
					ui64 = _uint64_interleave(lat-lat_grid, lon-lon_grid)
					ranges.append((ui64, ui64 + (1<<(64-precision))))
				ui64 = _uint64_interleave(lat, lon-lon_grid)
				ranges.append((ui64, ui64 + (1<<(64-precision))))
				ui64 = _uint64_interleave(lat+lat_grid, lon-lon_grid)
				ranges.append((ui64, ui64 + (1<<(64-precision))))
	
	ranges.sort()
	
	# merge the conditions
	shrink = []
	prev = None
	for i in ranges:
		if prev:
			if prev[1] != i[0]:
				shrink.append(prev)
				prev = i
			else:
				prev = (prev[0], i[1])
		else:
			prev = i
	
	shrink.append(prev)
	
	ranges = []
	for i in shrink:
		a,b=i
		if a == 0:
			a = None # we can remove the condition because it is the lowest value
		if b == 0x10000000000000000:
			b = None # we can remove the condition because it is the highest value
		
		ranges.append((a,b))
	
	return ranges


# In[69]:


def authenticate(keyspace):
    auth = PlainTextAuthProvider(username='cassandra',password='cassandra')
    cluster = Cluster(['172.16.117.201', '172.16.117.204', '172.16.117.152', '172.16.117.157'],auth_provider=auth)
    connection= cluster.connect(keyspace)
    return connection
def location_insert_data(date,lat,long):
    q ="""INSERT INTO processed_keyspace.missing_lat_long (created_date , lat , long ) VALUES ( '"""+date+"""',"""+str(lat)+""","""+str(long)+""") ;"""
    k = 'processed_keyspace'
    connection=authenticate(k)
    connection.execute(q)

def location_data(geohash):
    q ="""SELECT country,state,city from geohash_info where geohash = '"""+geohash+"""';"""
    k = 'processed_keyspace'
    connection=authenticate(k)
    location_data_ = connection.execute(q)

    return list(location_data_)


# input = 'jorhat'
# output = ['india','assam']

def find_state_country(city):
    q ="""SELECT country,state from city_state where city = '"""+city+"""';"""
    k = 'processed_keyspace'
    connection=authenticate(k)
    city_state_ = connection.execute(q)
    l = [[i.country,i.state] for i in city_state_][0]
    return list(l)


# In[70]:


def check_location(date,lat,long,city,country,state):
    location_info = location_data(encode(lat,long,6))
    
    if location_info:
        for location_info_ in location_info:
            city_ = location_info_.city
            state_ = location_info_.state
            country_ = location_info_.country
            
    else:
        location_insert_data(date,lat,long)
        if city and not state:
            country_state_ = find_state_country(city)
            country_ = country_state_[0]
            state_ = country_state_[1]
            city_ = city
        else:
            city_ = city
            state_ = state
            country_ = country

    return "^"+city_,"^"+state_,"^"+country_


def check_location_lat_lng(date,lat,long,city,country,state):
    
    location_info = location_data(encode(lat,long,6))
    
    if location_info:
        for location_info_ in location_info:
            city_ = location_info_.city
            state_ = location_info_.state
            country_ = location_info_.country
            
    else:
        location_insert_data(date,lat,long)
        if city and not state:
            country_state_ = find_state_country(city)
            country_ = country_state_[0]
            state_ = country_state_[1]
            city_ = city
        else:
            city_ = city
            state_ = state
            country_ = country

    return (lat, long, "^"+city_, "^"+state_, "^"+country_)


#------------------------------------location end-------------------------------------------


"""
Process functions to check rdd output

"""

def process1(time,rdd):

	print("1st dstream")
	
	if not rdd.isEmpty():
		print(rdd.collect())
		print("done")

def process2(time,rdd):

	print("2nd dstream")
	
	if not rdd.isEmpty():
		print(rdd.collect())
		print("done")

def process3(time,rdd):

	print("3rd dstream")
	
	if not rdd.isEmpty():
		print(rdd.collect())
		print("done")

def process4(time,rdd):

	print("3rd dstream")
	
	if not rdd.isEmpty():
		print(rdd.collect())
		print("done")


"""
Function to perform cartesian like product on elements of a list
:param: a list ex:[1,2,3]
:output : [(1,2),(1,3),(2,1),(2,3)] #note (1,1),(2,2) or (3,3) will not be include. 

"""

# def cartesian(data):
# 	try:
# 		result = list((data[i], data[j]) for i in range(len(data))for j in range(len(data)) if data[i] != data[j])
# 		return result

# 	except:
# 		return (['dummy',"dummy2"])

# def cartesian_with_sentiment(data):

# 	try:

# 		sentiment = tweet_object.get_sentiment_tag(data)
# 		result = list((data[i], data[j], sentiment) for i in range(len(data))for j in range(len(data)) if data[i] != data[j])
# 		return result

# 	except:
# 		return (['dummy',"dummy2"])

def cartesian_with_location(data):

	try:

		location = parse_location_place(data)
		result = list((location, data[i], data[j]) for i in range(len(data))for j in range(len(data)) if data[i] != data[j])
		return result

	except:
		return (['dummy',"dummy2"])

def cartesian(data):
	try:
		result = list((data[i], data[j]) for i in range(len(data))for j in range(len(data)) if data[i] != data[j])
		return result

	except:
		return []

"""
Function to get co-occuring hashtag and tweet id 
:param: a json
:output: will return the co-occuring hashtag and tweet id
"""

def get_co_occuring_hash_tag_with_tweetid(data):

	try:
		tweet_id = tweet_object.get_tweet_id(data)
		hashtag_list = tweet_object.get_hashtags(data)
		co_occuring_result = list((hashtag_list[i], hashtag_list[j]) for i in range(len(hashtag_list)) \
			for j in range(len(hashtag_list)) if hashtag_list[i]!=hashtag_list[j])	
		combine_co_occur_result = co_occuring_result
		ids =[[tweet_id]] * len(combine_co_occur_result)
		combine_ids_hash = zip(combine_co_occur_result,ids)	
		return combine_ids_hash
	except:
		e = sys.exc_info()[0]
		print("Error: %s" % e)
		return []

"""
Function to get mention with hashtag
:param: a json
:output: will return the mention with hashtag
"""

def get_mention_with_hashtag(data):

	try:
		mention_list = tweet_object.get_mentions(data)
		hashtag_list = tweet_object.get_hashtags(data)
		mentions_list_screen_name =[]
		for i in mention_list:
			mentions_list_screen_name.append(i['screen_name'])
		if hashtag_list == [] or mention_list == []:
			return []
		else:
			hashtag_mentions= list((hashtag_list[i], mentions_list_screen_name[j]) for i in range(len(hashtag_list))for j in range(len(mentions_list_screen_name)))
			mentions_hashtag= list((mentions_list_screen_name[i],hashtag_list[j]) for i in range(len(mentions_list_screen_name))for j in range(len(hashtag_list)))
			combine_hashtag_mention = hashtag_mentions+mentions_hashtag
			return combine_hashtag_mention
	except:
		e = sys.exc_info()[0]
		print("Error: %s" % e)
		return []


"""
Function to get mention with hashtag and tweet id
:param: a json
:output: will return the mention with hashtag and tweet id
"""

def get_mention_hashtag_with_tweetid(data):

	try:
		tweet_id = tweet_object.get_tweet_id(data)
		mention_hashtag = get_mention_with_hashtag(data)

		if mention_hashtag == []:
			return []
		else:

			ids =[[tweet_id]] * len(mention_hashtag)
			mentions_hashtag_tweet_id = zip(mention_hashtag,ids)
			return mentions_hashtag_tweet_id

	except:
		e = sys.exc_info()[0]
		print("Error: %s" % e)
		return []



"""
Function to get mention with keyword
:param: a json
:output: will return the mention with keyword
"""

def get_mention_keyword(data):

	try:


		combine_keywords_list = tweet_object.get_keywords(data)
		mentions_list = tweet_object.get_mentions(data)
		mentions_list_screen_name =[]
		for i in mentions_list:
			mentions_list_screen_name.append(i['screen_name'])

		if combine_keywords_list == [] or mentions_list_screen_name == []:
			return []
		else:
			key_mentions = list(product(combine_keywords_list,mentions_list_screen_name))
			mentions_key = list(product(mentions_list_screen_name,combine_keywords_list))
			combine_key_mention = key_mentions+mentions_key
			return combine_key_mention
	except:
		e = sys.exc_info()[0]
		print("Error: %s" % e)
		return []




"""
Function to get mention with keyword and tweet id
:param: a json
:output: will return the mention with keyword and tweet id
"""


def get_mention_keyword_tweetid(data):

	try:

		tweet_id = tweet_object.get_tweet_id(data)
		combine_km_mk = get_mention_keyword(data)

		if combine_km_mk == []:
			return []
		else:
			ids = [[tweet_id]] * len(combine_km_mk)
			mention_keyword_tweet_id = zip(combine_km_mk,ids)
			return mention_keyword_tweet_id
	except:
		e = sys.exc_info()[0]
		print("Error: %s" % e)
		return []



"""
Function to get mention 
:param: a json
:output: will return the mention 
"""


def get_top_mentions(data):
		
	try:

		mention_list = tweet_object.get_mentions(data)
		mentions_list_screen_name =[]

		if mention_list == []:

			return []

		else: 

			for i in mention_list:
				mentions_list_screen_name.append(i['screen_name'])
			return mentions_list_screen_name

	except:
		e = sys.exc_info()[0]
		print("Error: %s" % e)
		return []

"""
Function to get co - occuring mention with tweetid
:param: a json
:output: will return the co occuring mention with their tweet id
"""

def get_top_co_occuring_mention_with_tweet_id(data):

	try:

		mentions_list_screen_name = get_top_mentions(data)
		tweet_id = tweet_object.get_tweet_id(data)

		if mentions_list_screen_name == []:

			return []

		else:


			co_occuring_mention_result = list((mentions_list_screen_name[i], mentions_list_screen_name[j]) for i in range (len(mentions_list_screen_name)) \
				for j in range(len(mentions_list_screen_name)) if mentions_list_screen_name[i] != mentions_list_screen_name[j])
			#co_occuring_result1 = list((mentions_list1[j], mentions_list1[i]) for i in range (len(mentions_list1)) for j in range(len(mentions_list1)) if mentions_list1[i] != mentions_list1[j])
			ids = [[tweet_id]] * len(co_occuring_mention_result)
			combine_top_co_mentions_id = zip(co_occuring_mention_result, ids)
			return combine_top_co_mentions_id

	except:
		e = sys.exc_info()[0]
		print("Error: %s" % e)
		return []

"""
Function to get hashtag with user 
:param: a json
:output: will return hashtag with user
"""
def get_hastag_with_user(data):

	try:

		combine_hashtags_list = tweet_object.get_hashtags(data)
		user_id = tweet_object.get_user(data)


		if combine_hashtags_list == [] or user_id == []:

			return []

		else:

			ids = user_id * len(combine_hashtags_list)
			hashtag_tweet_user_id = zip(combine_hashtags_list,ids)
			user_id_tweet_hashtag = zip(ids,combine_hashtags_list)
			combine_hashtag_user_id = hashtag_tweet_user_id+user_id_tweet_hashtag
			return combine_hashtag_user_id	

	except:
		e = sys.exc_info()[0]
		print("Error: %s" % e)
		return []


"""
Function to get hashtag with user and tweet id 
:param: a json
:output: will return the hashtag with user and tweet id
"""

def get_hastag_with_user_tweet_id(data):

	try:

		combine_hashtag_user_id = get_hastag_with_user(data)
		tweet_id = tweet_object.get_tweet_id(data)
		if combine_hashtag_user_id == []:
			return []
		else:
			tids =[[tweet_id]] * len(combine_hashtag_user_id)
			hashtag_tweet_user_tweetid = zip(combine_hashtag_user_id,tids)
			return hashtag_tweet_user_tweetid

	except:
		e = sys.exc_info()[0]
		print("Error: %s" % e)
		return []


"""
Function to get mention with user
:param: a json
:output: will return the mention 
"""
def get_mention_user(data):		#v1 added			#5 #done

	try:

		user_id = tweet_object.get_user(data)
		mentions_list_screen_name = get_top_mentions(data)

		if user_id==[] or mentions_list_screen_name == []:

			return []

		else:

			user_ids = user_id * len(mentions_list_screen_name)
			mention_user_id = zip(mentions_list_screen_name,user_ids)
			user_id_mention = zip(user_ids,mentions_list_screen_name)
			combine_mu_um = mention_user_id+user_id_mention
			return combine_mu_um

	except:
		e = sys.exc_info()[0]
		print("Error: %s" % e)
		return []


"""
Function to get mention with user and tweet id
:param: a json
:output: will return the mention with user and tweet id
"""

def get_mention_with_user_tweet_id(data):			#5  #done

	try:
		combine_mu_um = get_mention_user(data)
		tweet_id = tweet_object.get_tweet_id(data)
		if combine_mu_um == []:
			return []
		else:
			ids = [[tweet_id]]* len(combine_mu_um)
			mention_user_tweetid = zip(combine_mu_um,ids)
			return mention_user_tweetid

	except:
		e = sys.exc_info()[0]
		print("Error: %s" % e)
		return []


"""
Function to get keyword with hashtag
:param: a json
:output: will return keyword with hashtag
"""

def get_key_hashtag(data):		#v1 added	#6   #done

	try:

		combine_keywords_list = tweet_object.get_keywords(data)
		combine_hashtags_list = tweet_object.get_hashtags(data)
		if combine_keywords_list == [] or combine_hashtags_list == []:

			return []
			
		else:

			hashtag_key = list(product(combine_keywords_list,combine_hashtags_list))
			key_hashtag = list(product(combine_hashtags_list,combine_keywords_list))
			combine_hashtag_key = hashtag_key+key_hashtag
			return combine_hashtag_key

	except:
		e = sys.exc_info()[0]
		print("Error: %s" % e)
		return []


"""
Function to get keyword with hashtag and tweetid
:param: a json
:output: will return keyword with hashtag and tweet id 
"""


def get_keyhashtag_tweetid(data):		#v1 done		#6  #done

	try:

		combine_key_hashtag = get_key_hashtag(data)
		tweet_id = tweet_object.get_tweet_id(data)
		if combine_key_hashtag == []:
			return []
		else:
			ids = [[tweet_id]]* len(combine_key_hashtag)
			hashtag_keyword_tweetid = zip(combine_key_hashtag,ids)
			return hashtag_keyword_tweetid

	except:
		e = sys.exc_info()[0]
		print("Error: %s" % e)
		return []


"""
Function to get keyword with user
:param: a json
:output: will return keyword with user 
"""


def get_keyword_with_user(data): 	#v1 done		#8 #done

	
	try:
		combine_keywords_list = tweet_object.get_keywords(data)
		user_id = tweet_object.get_user(data)

		if combine_keywords_list == []:
			return []
		else:
			ids = user_id * len(combine_keywords_list)
			keyword_user_id = zip(combine_keywords_list,ids)
			user_id_keyword = zip(ids,combine_keywords_list)
			combine_keyword_user_id = keyword_user_id+user_id_keyword

			return combine_keyword_user_id

	except:

		e = sys.exc_info()[0]
		print("Error: %s" % e)
		return combine_keywords_list


"""
Function to get keyword with user and tweet id
:param: a json
:output: will return keyword with user and tweet id
"""


def get_keyword_with_user_tweet_id(data):	#v1 done	#8 		#done

	try:
		combine_keywords_user_id = get_keyword_with_user(data)
		tweet_id = tweet_object.get_tweet_id(data)

		if combine_keywords_user_id == []:

			return []

		else:

			ids_tweet = [[tweet_id]] * len(combine_keywords_user_id)
			keyword_tweet_user_tweetid = zip(combine_keywords_user_id,ids_tweet)
			return keyword_tweet_user_tweetid

	except:

		e = sys.exc_info()[0]
		return []


#-------------------------------------token count-----------------------------


"""
Function to get hashtag with tweet id
:param: a json
:output: will return the hashtag with tweet id
"""

def get_hash_tag_with_tweetid(data):

	try:

		tweet_id = tweet_object.get_tweet_id(data)
		combine_hashtags_list = tweet_object.get_hashtags(data)

		ids =[[tweet_id]] * len(combine_hashtags_list)
		combine_hashtag_id = zip(combine_hashtags_list,ids)

		return combine_hashtag_id 	

	except:

		e = sys.exc_info()[0]
		print("Error: %s" % e)


"""
Function to get mention with tweet id
:param: a json
:output: will return the mention with tweet id
"""

def get_top_mentions_with_tweetid(data):
	
	try:

		tweet_id = tweet_object.get_tweet_id(data)
		mentions_list = get_top_mentions(data)

		if mentions_list == []:

			return []

		else: 

			ids = [[tweet_id]] * len(mentions_list)
			top_mentions_tweet_id = zip(mentions_list,ids)

			return top_mentions_tweet_id

	except:

		e = sys.exc_info()[0]
		print("Error: %s" % e)
		return []


"""
Function to get user with tweet id
:param: a json
:output: will return the user with tweet id
"""

def get_user_details_tweet_id(data):


	try:
		
		uid = tweet_object.get_user(data)
		tweet_id = tweet_object.get_tweet_id(data)

		return [(uid[0], [tweet_id])]

	except:

		e = sys.exc_info()[0]
		print("Error: %s" % e)


"""
Function to get keyword with tweet id
:param: a json
:output: will return keyword with tweet id
"""

def get_keyword_with_tweet_id(data):

	try:

		tweet_id = tweet_object.get_tweet_id(data)
		combine_keywords_list = tweet_object.get_keywords(data)

		ids=[[tweet_id]]*len(combine_keywords_list)
		keyword_with_id = zip(combine_keywords_list,ids)

		return keyword_with_id

	except:

		e = sys.exc_info()[0]
		print("Error: %s" % e)

def get_category(data):

	sentiment = tweet_object.get_sentiment_tag(data)
	security = tweet_object.get_security_tag(data)
	communal = tweet_object.get_communal_tag(data)
	category=0
	if (sentiment == 0 and security == 0  and communal == 1) or (sentiment == 0 and security == 2 and communal == 1): #pos
		category = '001'
	elif sentiment == 1 and security == 0 and communal == 1 or (sentiment == 1 and security == 2 and communal == 1): # neg
		category = '002'
	elif sentiment == 2 and security == 0 and communal == 1 or (sentiment == 2 and security == 2 and communal == 1): # neu
		category = '003'
	elif sentiment == 0 and security == 0 and communal == 0 or (sentiment == 0 and security == 2 and communal == 0): # com pos
		category = '011'
	elif sentiment == 1 and security == 0 and communal == 0 or (sentiment == 1 and security == 2 and communal == 0): # com neg
		category = '012'
	elif sentiment == 2 and security == 0 and communal == 0 or (sentiment == 2 and security == 2 and communal == 0): # com neutral
		category = '013'
	elif sentiment == 0 and security == 1 and communal == 1: # sec pos
		category = '101'
	elif sentiment == 1 and security == 1 and communal == 1: # sec neg
		category = '102'
	elif sentiment == 2 and security == 1 and communal == 1: # sec neu
		category = '103'
	elif sentiment == 0 and security == 1 and communal == 0: # com sec pos
		category = '111'
	elif sentiment == 1 and security == 1 and communal == 0: # com sec neg
		category = '112'
	elif sentiment == 2 and security == 1 and communal == 0: # com sec neu
		category = '113'
	else:
		category = str(sentiment)+str(security)+str(communal)+"9"

	return category

def get_category_dummy(data):
	sentiment = tweet_object.get_sentiment_tag(data)
	security = tweet_object.get_security_tag(data)
	communal = tweet_object.get_communal_tag(data)
	if sentiment == 0 and (security == 2 or security ==0) and communal == 1: #pos
		category = '001'
	elif sentiment == 1 and (security == 2 or security ==0) and communal == 1: # neg
		category = '002'
	elif sentiment == 2 and (security == 2 or security ==0) and communal == 1: # neu
		category = '003'
	elif sentiment == 0 and (security == 2 or security ==0) and communal == 0: # com pos
		category = '102'
	elif sentiment == 1 and (security == 2 or security ==0) and communal == 0: # com neg
		category = '012'
	elif sentiment == 2 and (security == 2 or security ==0) and communal == 0: # com neutral
		category = '013'
	elif sentiment == 0 and security == 1 and communal == 1: # sec pos
		category = '101'
	elif sentiment == 1 and security == 1 and communal == 1: # sec neg
		category = '102'
	elif sentiment == 2 and security == 1 and communal == 1: # sec neu
		category = '103'
	elif sentiment == 0 and security == 1 and communal == 0: # com sec pos
		category = '111'
	elif sentiment == 1 and security == 1 and communal == 0: # com sec neg
		category = '112'
	elif sentiment == 2 and security == 1 and communal == 0: # com sec neu
		category = '113'

	return category

#for python 2.7
def flatten_2(l):
	for el in l:
		if isinstance(el, collections.Iterable) and not isinstance(el, basestring):
			for sub in flatten_2(el):
				yield sub
		else:
			yield el
#for python 3
# def flatten_3(l):
#     for el in l:
#         if isinstance(el, collections.Iterable) and not isinstance(el, (str, bytes)):
#             yield from flatten(el)
#         else:
#             yield el
def flatten_tuple(tpl):
	if isinstance(tpl, (tuple, list)):
		for v in tpl:
			for sub in flatten_tuple(v):
				yield sub
# ...             yield from flatten(v)
	else:
		yield tpl

def data_formator(data):
	count_list =[0]*12
	tid_list = [['0']]*12
	hashtag = data[0]
	# try:
	if type(data[1][0][0]) == str:
		if data[1][0][0]=='001':
			index =0
		if data[1][0][0]=='002':
			index =1
		if data[1][0][0]=='003':
			index = 2
		if data[1][0][0]=='011':
			index =3
		if data[1][0][0]=='012':
			index =4
		if data[1][0][0]=='013':
			index =5
		if data[1][0][0]=='101':
			index =6
		if data[1][0][0]=='102':
			index =7
		if data[1][0][0]=='103':
			index =8
		if data[1][0][0]=='111':
			index =9
		if data[1][0][0]=='112':
			index =10
		if data[1][0][0]=='113':
			index =11
		count_list[index] = data[1][0][1]
		# try:
		if len(data[1][1][1]) == 1:
			tid_list[index]=data[1][1][1]
		else:
			tid_list[index]=list(flatten_tuple(data[1][1][1]))
		return(hashtag,count_list,tid_list)
		# except:
		# 	return("rahul"+str(data))
	if type(data[1][0][0]) == tuple:
		for x,y in zip(data[1][0],data[1][1]):
			if x[0]=='001':
				count_list[0]=x[1]
			if x[0]=='002':
				count_list[1]=x[1]
			if x[0]=='003':
				count_list[2]=x[1]
			if x[0]=='011':
				count_list[3]=x[1]
			if x[0]=='012':
				count_list[4]=x[1]
			if x[0]=='013':
				count_list[5]=x[1]
			if x[0]=='101':
				count_list[6]=x[1]
			if x[0]=='102':
				count_list[7]=x[1]
			if x[0]=='103':
				count_list[8]=x[1]
			if x[0]=='111':
				count_list[9]=x[1]
			if x[0]=='112':
				count_list[10]=x[1]
			if x[0]=='113':
				count_list[11]=x[1]


			if len(y[1])== 1:
				tid_l = y[1]
			else:
				# tid_ll=[element for tupl in y[1] for element in tupl]
				# tid_l = list(flatten_2(tid_ll))
				tid_l=list(flatten_tuple(y[1]))
			if y[0]=='001':
				tid_list[0] =list(tid_l)
			if y[0]=='002':
				tid_list[1] =list(tid_l)
			if y[0]=='003':
				tid_list[2] =list(tid_l)
			if y[0]=='011':
				tid_list[3] =list(tid_l)
			if y[0]=='012':
				tid_list[4] =list(tid_l)
			if y[0]=='013':
				tid_list[5] =list(tid_l)
			if y[0]=='101':
				tid_list[6] =list(tid_l)
			if y[0]=='102':
				tid_list[7] =list(tid_l)
			if y[0]=='103':
				tid_list[8] =list(tid_l)
			if y[0]=='111':
				tid_list[9]=list(tid_l)
			if y[0]=='112':
				tid_list[10] =list(tid_l)
			if y[0]=='113':
				tid_list[11] =list(tid_l)

		return(hashtag,count_list,tid_list)


	# except:
	# 	print("no data")
		
		

def data_formator_location(data):
	count_list =[0]*12
	tid_list = [['0']]*12
	token = data[0][1]
	location = data[0][0]
	# try:
	if type(data[0][2]) == str:
		if data[0][2]=='001':
			index =0
		if data[0][2]=='002':
			index =1
		if data[0][2]=='003':
			index = 2
		if data[0][2]=='011':
			index =3
		if data[0][2]=='012':
			index =4
		if data[0][2]=='013':
			index =5
		if data[0][2]=='101':
			index =6
		if data[0][2]=='102':
			index =7
		if data[0][2]=='103':
			index =8
		if data[0][2]=='111':
			index =9
		if data[0][2]=='112':
			index =10
		if data[0][2]=='113':
			index =11
		count_list[index] = data[1][0]
		# try:
		if len(data[1][1]) == 1:
			tid_list[index]=data[1][1]
		else:
			tid_list[index]=list(flatten_tuple(data[1][1]))
		return(location, token,count_list,tid_list)
		


	


#----------------------------------category----------------------
def get_category_hashtag(data):

	try:
		combine_hashtags_list = tweet_object.get_hashtags(data)
		category = get_category(data)
		category_list =[category] * len(combine_hashtags_list)
		combine_sentiment_hash = zip(combine_hashtags_list,category_list)
		return combine_sentiment_hash	

        # >>> combine_hashtags_list = ["#india", "#pakista"]
        # >>> category = "101"
        # >>> len(combine_hashtags_list)
        # 2
        # >>> category_list =[category] * len(combine_hashtags_list)
        # >>> category_list
        # ['101', '101']
        # >>> combine_sentiment_hash = zip(combine_hashtags_list,category_list)
        # >>> 
        # >>> combine_sentiment_hash
        # <zip object at 0x7f98892bc4c0>
        # >>> print(set(combine_sentiment_hash))
        # {('#pakista', '101'), ('#india', '101')}

	except:

		e = sys.exc_info()[0]
		print("Error: %s" % e)
		return []

def get_category_hashtag_tidlist(data):

	try:
		tweet_id = tweet_object.get_tweet_id(data)
		combine_category_hash = get_category_hashtag(data)
	
		ids =[[tweet_id]] * len(combine_category_hash)
		combine_category_ids_hash = zip(combine_category_hash,ids)

		return combine_category_ids_hash	

	except:
		e = sys.exc_info()[0]
		print("Error: %s" % e)
		return []

def get_category_location(data):

	try:
		combine_location_list = parse_location(data)
		category = get_category(data)
		category_list =[category] * len(combine_location_list)
		combine_location = zip(combine_location_list,category_list)
		return combine_location

	except:

		e = sys.exc_info()[0]
		print("Error: %s" % e)
		return []

def get_category_location_tidlist(data):

	try:
		tweet_id = tweet_object.get_tweet_id(data)
		combine_category = get_category_location(data)
		ids =[[tweet_id]] * len(combine_category)
		combine_category_ids_locatn = zip(combine_category,ids)
		return combine_category_ids_locatn	

	except:
		e = sys.exc_info()[0]
		print("Error: %s" % e)
		return []
"""
Function to get sentiment with corresponding keyword
:param: a json
:output: will return sentiment with corresponding keyword
"""

def get_category_keyword(data):

	
	try:
		combine_keywords_list = tweet_object.get_keywords(data)
		category = get_category(data)
		category_list =[category] * len(combine_keywords_list)
		combine_category_key = zip(combine_keywords_list,category_list)
		return combine_category_key

	except:

		e = sys.exc_info()[0]
		print("Error: %s" % e)
		return []



"""
Function to get sentiment with corresponding keyword and tweet id list
:param: a json
:output: will return sentiment with corresponding keyword and tweet id list
"""


def get_category_keyword_tweetidlist(data):

	try:
		tweet_id = tweet_object.get_tweet_id(data)
		combine_category_keyword = get_category_keyword(data)
		ids =[[tweet_id]] * len(combine_category_keyword)
		combine_category_ids_keyword = zip(combine_category_keyword,ids)

		return combine_category_ids_keyword

	except:

		e = sys.exc_info()[0]
		print("Error: %s" % e)
		return []

"""
Function to get sentiment with corresponding user
:param: a json
:output: will return sentiment with corresponding user
"""

def get_user_category(data):

	try:
		category = get_category(data)
		user_id = tweet_object.get_user(data)

		return [(user_id[0],category)]

	except:
		e = sys.exc_info()[0]
		print("Error: %s" % e)
		return []


"""
Function to get sentiment with corresponding hashtag
:param: a json
:output: will return sentiment with corresponding hashtag
"""

def get_user_category_tweetid(data):

	
	try:
		category = get_category(data)
		user_id = tweet_object.get_user(data)
		tweet_id = tweet_object.get_tweet_id(data)
		return [((user_id[0],category),[tweet_id])]
	except:
		e = sys.exc_info()[0]
		print("Error: %s" % e)
		return []
"""
Function to get sentiment with mention
:param: a json
:output: will return sentiment with mention
"""


def get_category_mention(data):

	try:
		mentions_list = get_top_mentions(data)
		category = get_category(data)
		category_list =[category] * len(mentions_list)
		combine_category_mention = zip(mentions_list,category_list)

		return combine_category_mention

	except:

		e = sys.exc_info()[0]
		print("Error: %s" % e)
		return []

"""
Function to get sentiment with mention and tweet id list
:param: a json
:output: will return sentiment with mention and tweet id list
"""



def get_category_mention_tweetidlist(data):
	try:
		tweet_id = tweet_object.get_tweet_id(data)
		combine_category_mention_list = get_category_mention(data)
		ids = [[tweet_id]] *len(combine_category_mention_list)
		combine_mention_tweetid_category = zip(combine_category_mention_list,ids)
		return combine_mention_tweetid_category

	except:

		e = sys.exc_info()[0]
		print("Error: %s" % e)
		return []

def get_hash_hash_category(data):
	try:
		hash_list = tweet_object.get_hashtags(data)
		if hash_list == []:
			return []
		else:
			category = get_category(data)
			hash_hash_list = cartesian(hash_list)
			category_list = [category]*len(hash_hash_list)
			hash_hash_category = zip(hash_hash_list,category_list)
			#[((#1,#2),category),((#3,#4),category)]
			return hash_hash_category 
	except:
		return []

def get_hash_hash_category_tid(data):
	try:
		hash_hash_category_list = get_hash_hash_category(data)
		tweetid = tweet_object.get_tweet_id(data)
		if hash_hash_category_list == []:
			return []
		else:
			tid_list = [[tweetid]]*len(hash_hash_category_list)
			hash_hash_category_tid = zip(hash_hash_category_list,tid_list)
			#[(((#1,#2),category),[tid]),(((#3,#4),category),[tid])]
			return hash_hash_category_tid
	except:
		return []
"""
Function to get mention 
:param: a json
:output: will return the mention 
"""


def get_top_mentions(data):
		
	try:

		mention_list = tweet_object.get_mentions(data)
		mentions_list_screen_name =[]

		if mention_list == []:

			return []

		else: 

			for i in mention_list:
				mentions_list_screen_name.append(i['screen_name'])
			return mentions_list_screen_name

	except:
		e = sys.exc_info()[0]
		print("Error: %s" % e)
		return []
def get_mention_mention_category(data):
	try:
		mention_list = get_top_mentions(data)
		# category = get_category(data)
		if mention_list == [] or len(mention_list)==1 :
			return []
		else:
			category = get_category(data)
			mention_mention_list = cartesian(mention_list)
			category_list = [category]*len(mention_mention_list)
			mention_mention_category = zip(mention_mention_list,category_list)
			#[((@1,@2),category),((@3,@4),category)]
			return mention_mention_category
	except:
		return []

def get_mention_mention_tid(data):
	try:
		mention_mention_category_list = get_mention_mention_category(data)
		tweetid = tweet_object.get_tweet_id(data)
		if mention_mention_category_list == []:
			return []
		else:
			tid_list = [[tweetid]]*len(mention_mention_category_list)
			mention_mention_category_tid = zip(mention_mention_category_list,tid_list)
			#[(((@1,@2),category),[tid]),(((@3,@4),category),[tid])]
			return mention_mention_category_tid
	except:
		return []

def get_hash_mention_category(data):
	try:
		mention_list = get_top_mentions(data)
		hashtag_list = tweet_object.get_hashtags(data)
		
		if hashtag_list == [] or mention_list == []:
			return []
		else:
			category = get_category(data)
			hashtag_mentions= list((hashtag_list[i], mention_list[j]) for i in range(len(hashtag_list))for j in range(len(mention_list)))
			mentions_hashtag= list((mention_list[i],hashtag_list[j]) for i in range(len(mention_list))for j in range(len(hashtag_list)))
			combine_hashtag_mention = hashtag_mentions+mentions_hashtag
			category_list = [category]*len(combine_hashtag_mention)
			combine_hashtag_mention_category = zip(combine_hashtag_mention,category_list)
			return combine_hashtag_mention_category
	except Exception as e:
		# e = sys.exc_info()[0]
		print("Error: %s" % e)
		return []
def get_hash_mention_category_tid(data):
	try:
		hash_mention_category_list = get_hash_mention_category(data)
		tid = tweet_object.get_tweet_id(data)
		if hash_mention_category_list == []:
			return []
		else:
			tid_list = [[tid]]*len(hash_mention_category_list)
			hash_mention_category_tid = zip(hash_mention_category_list,tid_list)
			return hash_mention_category_tid

	except Exception as e:
		# e = sys.exc_info()[0]
		print("Error: %s" % e)
		return []

def get_hastag_with_user_category(data):

	try:

		combine_hashtags_list = tweet_object.get_hashtags(data)
		user_id = tweet_object.get_user(data)
		if combine_hashtags_list == [] or user_id == []:

			return []

		else:
			category = get_category(data)
			ids = user_id * len(combine_hashtags_list)

			hashtag_tweet_user_id = zip(combine_hashtags_list, ids)
			user_id_tweet_hashtag = zip(ids, combine_hashtags_list)
			combine_hashtag_user_id = hashtag_tweet_user_id+user_id_tweet_hashtag
			category_list = [category] * len(combine_hashtag_user_id)
			combine_hashtag_user_id_category = zip(combine_hashtag_user_id,category_list)
			return combine_hashtag_user_id_category

	except:
		e = sys.exc_info()[0]
		print("Error: %s" % e)
		return []

def get_hastag_with_user_category_tweet_id(data):

	try:

		hashtag_user_category = get_hastag_with_user_category(data)
		tweet_id = tweet_object.get_tweet_id(data)

		if hashtag_user_category == []:

			return []

		else: 

			ids = [[tweet_id]] * len(hashtag_user_category)
			hashtag_user_category_tweetid = zip(hashtag_user_category, ids)
			return hashtag_user_category_tweetid

	except:

		e = sys.exc_info()[0]
		print("Error: %s" % e)
		return []

def get_keyword_hashtag_category(data):

	try:

		combine_keywords_list = tweet_object.get_keywords(data)
		combine_hashtags_list = tweet_object.get_hashtags(data)
		

		if combine_keywords_list == [] or combine_hashtags_list == []:

			return []
			
		else:
			category = get_category(data)
			hashtag_key_category = list((combine_keywords_list[i], combine_hashtags_list[j]) for i in range(len(combine_keywords_list)) for j in range(len(combine_hashtags_list)))
			key_hashtag_category = list((combine_hashtags_list[i], combine_keywords_list[j]) for i in range(len(combine_hashtags_list)) for j in range(len(combine_keywords_list)))
			combine_hashtag_key = hashtag_key_category+key_hashtag_category
			category_list = [category]*len(combine_hashtag_key)
			combine_hashtag_key_category = zip(combine_hashtag_key,category_list)
			return combine_hashtag_key_category

	except:
		e = sys.exc_info()[0]
		print("Error: %s" % e)
		return []
	

def get_keyword_hashtag_category_tweetid(data):

	try:

		keyword_hashtag_category = get_keyword_hashtag_category(data)
		tweet_id = tweet_object.get_tweet_id(data)

		if keyword_hashtag_category == []:

			return []

		else:

			ids = [[tweet_id]] * len(keyword_hashtag_category)
			keyword_hashtag_category_tweetid = zip(keyword_hashtag_category, ids)

			return keyword_hashtag_category_tweetid

	except:

		e = sys.exc_info()[0]
		print("Error: %s" % e)
		return []

def get_mention_user_category(data):

	try:

		user_id = tweet_object.get_user(data)
		mentions_list_screen_name = get_top_mentions(data)
		

		if user_id==[] or mentions_list_screen_name == []:

			return []

		else:
			category = get_category(data)
			user_ids = user_id * len(mentions_list_screen_name)
			mention_user_id = zip(mentions_list_screen_name,user_ids)
			user_id_mention = zip(user_ids,mentions_list_screen_name)
			combine_mu_um = mention_user_id+user_id_mention
			category_list = [category] * len(combine_mu_um)
			combine_mu_um_category = zip(combine_mu_um,category_list)
			return combine_mu_um_category

	except:
		e = sys.exc_info()[0]
		print("Error: %s" % e)
		return []


def get_mention_with_user_category_tweet_id(data):

	try:

		mention_with_user_category = get_mention_user_category(data)
		tweet_id = tweet_object.get_tweet_id(data)
		if mention_with_user_category == []:
			return []
		else:
			ids = [[tweet_id]] * len(mention_with_user_category)
			mention_user_category_tweet_id = zip(mention_with_user_category, ids)
			return mention_user_category_tweet_id

	except:

		e = sys.exc_info()[0]
		print("Error: %s" % e)
		return []

def get_mention_keyword_category(data):

	try:

		combine_keywords_list = tweet_object.get_keywords(data)
		mentions_list_screen_name = get_top_mentions(data)
		
		if combine_keywords_list == [] or mentions_list_screen_name == []:
			
			return []

		else:
			category = get_category(data)
			key_mentions_sentiment = list((combine_keywords_list[i], mentions_list_screen_name[j]) for i in range(len(combine_keywords_list)) for j in range(len(mentions_list_screen_name)))
			mentions_key_sentiment = list((mentions_list_screen_name[i], combine_keywords_list[j]) for i in range(len(mentions_list_screen_name)) for j in range(len(combine_keywords_list)))
			combine_key_mention = key_mentions_sentiment+mentions_key_sentiment
			category_list = [category]*len(combine_key_mention)
			combine_key_mention_category = zip(combine_key_mention,category_list)
			return combine_key_mention_category

	except:
		e = sys.exc_info()[0]
		print("Error: %s" % e)
		return []


def get_mention_keyword_category_tweetid(data):

	try:

		mention_keyword_category = get_mention_keyword_category(data)
		tweet_id = tweet_object.get_tweet_id(data)
		if mention_keyword_category == []:
			return []
		else:
			ids =[[tweet_id]] * len(mention_keyword_category)
			mention_keyword_category_tweetid = zip(mention_keyword_category, ids)
			return mention_keyword_category_tweetid

	except:
		e = sys.exc_info()[0]
		print("Error: %s" % e)
		return []

def get_key_key_category(data):
	try:
		combine_keywords_list = tweet_object.get_keywords(data)
		if combine_keywords_list == []:
			return []
		else:
			category = get_category(data)
			key_key_list = cartesian(combine_keywords_list)
			category_list = [category]*len(key_key_list)
			key_key_category = zip(key_key_list,category_list)
			#[((*1,*2),category),((*3,*4),category)]
			return key_key_category 
	except:
		return []
def get_key_key_category_tid(data):
	try:
		key_key_category_list = get_key_key_category(data)
		tweetid = tweet_object.get_tweet_id(data)
		if key_key_category_list == []:
			return []
		else:
			tid_list = [[tweetid]]*len(key_key_category_list)
			key_key_category_tid = zip(key_key_category_list,tid_list)
			#[(((#1,#2),category),[tid]),(((#3,#4),category),[tid])]
			return key_key_category_tid
	except:
		return []

def get_keyword_with_user_category(data):

	try:
		combine_keywords_list = tweet_object.get_keywords(data)
		user_id = tweet_object.get_user(data)
		

		if combine_keywords_list == []:

			return []

		else:
			category = get_category(data)
			category_list = [category] * len(combine_keywords_list)
			ids = user_id * len(combine_keywords_list)
			keyword_user_id = zip(combine_keywords_list, ids)
			user_id_keyword = zip(ids, combine_keywords_list)
			combine_keyword_user_id = keyword_user_id+user_id_keyword
			combine_keyword_user_id_category = zip(combine_keyword_user_id,category_list)
			return combine_keyword_user_id_category

	except:

		e = sys.exc_info()[0]
		print("Error: %s" % e)
		return []


def get_keyword_with_user_category_tweet_id(data):

	try: 

		keyword_user_category = get_keyword_with_user_category(data)
		tweet_id = tweet_object.get_tweet_id(data)
		if keyword_user_category == []:
			return []
		else:
			ids = [[tweet_id]] * len(keyword_user_category)
			keyword_user_category_tweetid = zip(keyword_user_category, ids)
			return keyword_user_category_tweetid
	except:
		e = sys.exc_info()[0]
		print("Error: %s" % e)
		return []



def get_remap_data(data):
	try:
		new_data = (data[0][0],(data[0][1],data[1]))
		return new_data
	except:
		e = sys.exc_info()[0]
		print("Error: %s" % e)
		return []



def checker_category_token(data):
	try:
		# print("data")
		# print(data)
		check_data1 = data[0]
		# print("check_data1")
		# print(check_data1)
		check_data2 = data[0]
		# print("check_data2")
		# print(check_data2)
		check_data3 = data[1]
		# print("check_data2")
		# print(check_data2)
		return data
	except:
		return data
		# print("Error")
		# return (u'#dummy',[0,0,0,0,0,0,0,0,0,0,0,0],[0,0,0,0,0,0,0,0,0,0,0,0])
"""
Function to store token sentiment
:param: token sentiment rdd
"""

def checker_category_location(data):
	try:
		check_data1 = data[0][0] #lat
		check_data2 = data[0][1] #lng
		check_data3 = data[0][2] #city
		check_data4 = data[0][3] #state
		check_data5 = data[0][4] #country 
 		check_data6 = data[1] #token
		check_data7 = data[2] #count
		check_data8 = data[3] #tids

		return data

	except:
		return data






###############==============================================TOKEN C0-OCCUR================================================###############


##################################################################################
#####---------location parsing--------------####

"""
Function to parse location
:param: a json
:output: will return latitude longitude city state and country
"""

def parse_location(data):

	date_value=datetime.datetime.now().date()
	date_str=str(date_value.strftime('%Y-%m-%d'))
	location_new = tweet_object.get_tweet_location_info(data)	
	if location_new=={}:
		return []
	else:
		latitude = location_new['latitude']
		longitude = location_new['longitude']
		if latitude == None and longitude == None:
			return []
		else:
			city=location_new['city']
			state=location_new['state']
			country=location_new['country']
			if city=='dummy city' and state =='dummy state' and country==None:
				return []
			if city=='dummy city' and state =='dummy state' and country != None:
				return ["^"+country]
			if city=='dummy city' and state !='dummy state' and country != None:
				return ["^"+state,"^"+country]
			if city !='dummy city' and state!='dummy state' and country!=None:
				return ["^"+city,"^"+state,"^"+country]
			if city!='dummy city' and state=='dummy state' and country==None:
				parsed_location_list = list(check_location(date_str,latitude,longitude,city,None,None))
				return [parsed_location_list]
			if city!='dummy city' and state=='dummy state' and country!=None:
				return[]	



#-------------------------location_co_occur-------------------------------

def get_hash_location_category(data):
	try:
		location_list = parse_location(data)
		hashtag_list = tweet_object.get_hashtags(data)
		
		if hashtag_list == [] or location_list == []:
			return []
		else:
			category = get_category(data)
			hashtag_location= list((hashtag_list[i], location_list[j]) for i in range(len(hashtag_list))for j in range(len(location_list)))
			location_hashtag= list((location_list[i],hashtag_list[j]) for i in range(len(location_list))for j in range(len(hashtag_list)))
			combine_hashtag_location = hashtag_location+location_hashtag
			category_list = [category]*len(combine_hashtag_location)
			combine_hashtag_location_category = zip(combine_hashtag_location,category_list)
			return combine_hashtag_location_category
	except Exception as e:
		# e = sys.exc_info()[0]
		print("Error: %s" % e)
		return []
def get_hash_location_category_tid(data):
	try:
		hash_location_category_list = get_hash_location_category(data)
		tid = tweet_object.get_tweet_id(data)
		if hash_location_category_list == []:
			return []
		else:
			tid_list = [[tid]]*len(hash_location_category_list)
			hash_location_category_tid = zip(hash_location_category_list,tid_list)
			return hash_location_category_tid

	except Exception as e:
		# e = sys.exc_info()[0]
		print("Error: %s" % e)
		return []

def get_men_location_category(data):
	try:
		location_list = parse_location(data)
		mention_list = get_top_mentions(data)
		
		if mention_list == [] or location_list == []:
			return []
		else:
			category = get_category(data)
			mention_location= list((mention_list[i], location_list[j]) for i in range(len(mention_list))for j in range(len(location_list)))
			location_mention= list((location_list[i],mention_list[j]) for i in range(len(location_list))for j in range(len(mention_list)))
			combine_mention_location = mention_location+location_mention
			category_list = [category]*len(combine_mention_location)
			combine_mention_location_category = zip(combine_mention_location,category_list)
			return combine_mention_location_category
	except Exception as e:
		# e = sys.exc_info()[0]
		print("Error: %s" % e)
		return []
def get_men_location_category_tid(data):
	try:
		mention_location_category_list = get_men_location_category(data)
		tid = tweet_object.get_tweet_id(data)
		if mention_location_category_list == []:
			return []
		else:
			tid_list = [[tid]]*len(mention_location_category_list)
			mention_location_category_tid = zip(mention_location_category_list,tid_list)
			return mention_location_category_tid
	except Exception as e:
		# e = sys.exc_info()[0]
		print("Error: %s" % e)
		return []

def get_location_with_user_category(data):

	try:

		combine_location_list = parse_location(data)
		user_id = tweet_object.get_user(data)
		if combine_location_list == [] or user_id == []:
			return []

		else:
			category = get_category(data)
			ids = user_id * len(combine_location_list)
			location_tweet_user_id = zip(combine_location_list, ids)
			user_id_tweet_location = zip(ids, combine_location_list)
			combine_location_user_id = location_tweet_user_id+user_id_tweet_location
			category_list = [category] * len(combine_location_user_id)
			combine_location_user_id_category = zip(combine_location_user_id,category_list)
			return combine_location_user_id_category

	except:
		e = sys.exc_info()[0]
		print("Error: %s" % e)
		return []

def get_location_with_user_category_tweet_id(data):

	try:

		location_user_category = get_location_with_user_category(data)
		tweet_id = tweet_object.get_tweet_id(data)

		if location_user_category == []:

			return []

		else: 

			ids = [[tweet_id]] * len(location_user_category)
			location_user_category_tweetid = zip(location_user_category, ids)
			return location_user_category_tweetid

	except:

		e = sys.exc_info()[0]
		print("Error: %s" % e)
		return []

def get_keyword_location_category(data):

	try:

		combine_keywords_list = tweet_object.get_keywords(data)
		combine_location_list = parse_location(data)
		

		if combine_keywords_list == [] or combine_location_list == []:

			return []
			
		else:
			category = get_category(data)
			location_key_category = list((combine_keywords_list[i], combine_location_list[j]) for i in range(len(combine_keywords_list)) for j in range(len(combine_location_list)))
			key_location_category = list((combine_location_list[i], combine_keywords_list[j]) for i in range(len(combine_location_list)) for j in range(len(combine_keywords_list)))
			combine_location_key = location_key_category+key_location_category
			category_list = [category]*len(combine_location_key)
			combine_location_key_category = zip(combine_location_key,category_list)
			return combine_location_key_category

	except:
		e = sys.exc_info()[0]
		print("Error: %s" % e)
		return []
	

def get_keyword_location_category_tweetid(data):

	try:

		keyword_location_category = get_keyword_location_category(data)
		tweet_id = tweet_object.get_tweet_id(data)

		if keyword_location_category == []:

			return []

		else:

			ids = [[tweet_id]] * len(keyword_location_category)
			keyword_location_category_tweetid = zip(keyword_location_category, ids)

			return keyword_location_category_tweetid

	except:

		e = sys.exc_info()[0]
		print("Error: %s" % e)
		return []


#############################----LOCATION TOKEN----############################

def parse_cat_location(data):

	date_value=datetime.datetime.now().date()
	date_str=str(date_value.strftime('%Y-%m-%d'))
	location_new = tweet_object.get_tweet_location_info(data)	

	if location_new=={}:

		return []

	else:

		latitude = location_new['latitude']
		longitude = location_new['longitude']

		if latitude == None and longitude == None:

			return []

		else:

			city=location_new['city']
			state=location_new['state']
			country=location_new['country']
			if city=='dummy city' and state =='dummy state' and country==None:

				return []

			if city=='dummy city' and state =='dummy state' and country != None:

				parsed_location_list = (latitude, longitude, "^" + city, "^" + state, "^" + country)
				return [parsed_location_list]

			if city=='dummy city' and state !='dummy state' and country != None:

				parsed_location_list = (latitude, longitude, "^" + city, "^" + state, "^" + country)
				return [parsed_location_list]

			if city !='dummy city' and state!='dummy state' and country!=None:

				parsed_location_list = (latitude, longitude, "^" + city, "^" + state, "^" + country)
				return [parsed_location_list]

			if city!='dummy city' and state=='dummy state' and country==None:

				parsed_location_list = list(check_location_lat_lng(date_str,latitude,longitude,city,None,None))
				return [parsed_location_list]

			if city!='dummy city' and state=='dummy state' and country!=None:

				parsed_location_list = (latitude, longitude, "^" + city, "^" + state, "^" + country)
				return [parsed_location_list]


def get_cat_location_hashtag_place(data):
	
	try:

		data_location = parse_cat_location(data)
		combine_hashtags_list = tweet_object.get_hashtags(data)

		if data_location == [] or combine_hashtags_list == []:

			return []

		else:
			category = get_category(data)
			Locations = data_location * len(combine_hashtags_list)
			category_list = [category] * len(Locations)
			Locations_with_hashtag = zip(Locations,combine_hashtags_list,category_list)

			return Locations_with_hashtag

	except:

		e = sys.exc_info()[0]
		print("Error: %s" % e)
		return []



"""
Function to get location with hashtag and tweetid
:param: a json
:output: will return latitude longitude city state and country and hashtag
"""


def get_cat_hashtag_with_location_tweet_id_place(data):
	
	try:

		tweet_id = tweet_object.get_tweet_id(data)
		locations_with_hashtag = get_cat_location_hashtag_place(data)

		if locations_with_hashtag == []:

			return []

		else:

			ids = [[tweet_id]] * len(locations_with_hashtag)
			locations_with_hashtag_tweetid = zip(locations_with_hashtag,ids)

			return locations_with_hashtag_tweetid

	except:

		e = sys.exc_info()[0]
		print("Error: %s" % e)
		return []


#####---------location keyword--------------####

"""
Function to get location and keyword
:param: a json
:output: will return latitude longitude city state and country and keyword
"""


def get_cat_location_keyword_place(data):
	
	try:

		data_location = parse_cat_location(data)
		combine_keywords_list = tweet_object.get_keywords(data)

		if combine_keywords_list == [] or data_location == []:

			return []

		else:
			category = get_category(data)
			locations= data_location * len(combine_keywords_list)

			category_list = [category] * len(locations)
			locations_with_keyword = zip(locations, combine_keywords_list, category_list)
			return locations_with_keyword

	except:

		e = sys.exc_info()[0]
		print("Error: %s" % e)
		return []


"""
Function to get location with keyword and tweet id
:param: a json
:output: will return latitude longitude city state and country with keyword and tweet id
"""


def get_cat_keyword_with_location_tweet_id_place(data):

	try:

		tweet_id = tweet_object.get_tweet_id(data)
		locations_with_keyword = get_cat_location_keyword_place(data)

		if locations_with_keyword == []:

			return []

		else:

			ids = [[tweet_id]] * len(locations_with_keyword)
			locations_with_keyword_tweetid = zip(locations_with_keyword,ids)

			return locations_with_keyword_tweetid

	except:

		e = sys.exc_info()[0]
		print("Error: %s" % e)
		return []

"""
Function to get location and user
:param: a json
:output: will return latitude longitude city state and country and user
"""


def get_cat_location_user(data):

	try:

		data_user_location = parse_cat_location(data)
		user_id = tweet_object.get_user(data)

		if data_user_location == []:

			return []

		else:
			category = get_category(data)
			category_list = [category] * len(user_id)
			locations_with_user = zip(data_user_location,user_id,category_list)
			return locations_with_user

	except:

		e = sys.exc_info()[0]
		print("Error: %s" % e)
		return []


"""
Function to get location with user and tweet id
:param: a json
:output: will return latitude longitude city state and country with user and tweet id
"""

def get_cat_user_location_tweet_id(data):

	try:

		tweet_id = tweet_object.get_tweet_id(data)
		locations_with_user = get_cat_location_user(data)

		if locations_with_user == []:

			return []

		else:

			ids = [[tweet_id]] * len(locations_with_user)
			locations_with_user_tweet_id = zip(locations_with_user,ids)
			return locations_with_user_tweet_id

	except:

		e = sys.exc_info()[0]
		print("Error: %s" % e)
		return []

#####------------------location mention----------------#####


"""
Function to get location and mention
:param: a json
:output: will return latitude longitude city state and country with mention
"""

def get_cat_mention_location_place(data):

	try:

		data_user_location = parse_cat_location(data)
		mentions_list = get_top_mentions(data)
		category = get_category(data)

		if data_user_location == [] or mentions_list == []:

			return []

		else:

			locations = data_user_location * len(mentions_list)
			category_list = [category] * len(locations)
			locations_with_mentions = zip(locations,mentions_list,category_list)

			return locations_with_mentions

	except:

		e = sys.exc_info()[0]
		print("Error: %s" % e)
		return []
		

"""
Function to get location with mention and tweet id
:param: a json
:output: will return latitude longitude city state and country with mention and tweet id
"""

def get_cat_mention_with_location_tweet_id_place(data):

	try:

		tweet_id = tweet_object.get_tweet_id(data)
		locations_with_mentions = get_cat_mention_location_place(data)

		if locations_with_mentions == []:

			return []

		else:

			ids = [[tweet_id]] * len(locations_with_mentions)
			locations_with_mentions_tweetid = zip(locations_with_mentions,ids)

			return locations_with_mentions_tweetid


	except:

		e = sys.exc_info()[0]
		print("Error: %s" % e)
		return []



######################################################----STORE----#####################################################


def store_token_category_data(time,rdd): # previously window_sentiment_co_occuring_rdd

	try:

		if not rdd.isEmpty():
			print("Entered in token_category")

			sql_context = batchData.get_sql_context_instance()
			# today_date = datetime.datetime.utcnow().date()
			# today_date = today_date+timedelta(days=1)

			
			today_time = time+timedelta(hours=5,minutes=30)
			today_date = today_time.date()
			today_time = str(today_time.strftime('%H:%M:%S'))

			# today_time = datetime.datetime.strptime(today_date_time,'%H:%M:%S').time()
			# print(today_time)
			# print(type(today_time))
			check_rdd = rdd.map(checker_category_token)
			# data=check_rdd.map(lambda w:w[2])
			# print(data.collect())
			category_list = [1,2,3,11,12,13,101,102,103,111,112,113]
			row_rdd = check_rdd.map(lambda w: Row(created_date=today_date,created_time=today_time,token_name=w[0],category_class_list=category_list,count_list=w[1], tweetidlist=w[2]))
			dataframe = batchData.row_todf(sql_context,row_rdd)
			processed_dataframe = batchDataOperation.streaming_token_count(dataframe)
			#processed_dataframe.show(20,False)	
			# processed_dataframe.printSchema()		
			batchData.storedata("processed_keyspace",TOKEN_COUNT_TABLE,processed_dataframe)
			print("store_token_count=============================>Done")

	except Exception as e:
		# print(rdd.collect())
		
		print("Error: %s" % e)
		return []

"""
Function to store token sentiment
:param: token sentiment rdd
"""

def store_co_occurence_token_category_data(time,rdd): # previously window_sentiment_co_occuring_rdd

	try:

		if not rdd.isEmpty():
			print("Entered in toke_co_ccout")

			sql_context = batchData.get_sql_context_instance()
			# today_date = datetime.datetime.utcnow().date()
			# today_date = today_date+timedelta(days=1)	
			today_time = time+timedelta(hours=5,minutes=30)
			today_date = today_time.date()
			today_time = str(today_time.strftime('%H:%M:%S'))
			check_rdd = rdd.map(checker_category_token)
			
			# data=check_rdd.map(lambda w:w[2])
			# print(data.collect())
			category_list = [1,2,3,11,12,13,101,102,103,111,112,113]
			row_rdd = check_rdd.map(lambda w: Row(created_date=today_date,created_time=today_time,token_name1=w[0][0],token_name2=w[0][1],category_class_list=category_list,count_list=w[1], tweetidlist=w[2]))
			dataframe = batchData.row_todf(sql_context,row_rdd)
			processed_dataframe = batchDataOperation.streaming_co_occur(dataframe)
			#processed_dataframe.show()	
			# dataframe.show(10,False)
			batchData.storedata("processed_keyspace",TOKEN_CO_OCCUR_TABLE,processed_dataframe)
			print("store_token_co_occur=============================>Done")

	except Exception as e:
		# print(rdd.collect())
		
		print("Error: %s" % e)
		return []

def store_token_category_location(time,rdd): # previously window_sentiment_co_occuring_rdd

	try:

		if not rdd.isEmpty():
			print("Entered in token_location")

			sql_context = batchData.get_sql_context_instance()
			today_time = time+timedelta(hours=5,minutes=30)
			today_date = today_time.date()
			today_time = str(today_time.strftime('%H:%M:%S'))
			check_rdd = rdd.map(checker_category_location)
			
			category_list = [1,2,3,11,12,13,101,102,103,111,112,113]
			row_rdd = check_rdd.map(lambda w: Row(created_date=today_date,created_time=today_time, city=w[0][2], state=w[0][3], country=w[0][4],tweet_cl_latitude=w[0][0],tweet_cl_longitude=w[0][1], token_name=w[1],category_class_list=category_list,count_list=w[2], tweetidlist=w[3]))
			dataframe = batchData.row_todf(sql_context,row_rdd)
			processed_dataframe = batchDataOperation.streaming_token_location_place(dataframe)
			#processed_dataframe.show()	
			# dataframe.show(10,False)
			batchData.storedata("processed_keyspace",TOKEN_LOCATION_TABLE,processed_dataframe)
			print("Storing token location =============================>Done")

	except Exception as e:
		# print(rdd.collect())
		
		print("Error: %s" % e)
		return []



# xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx-----------------------------------------------------------------------xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx





def main():
	
	start_time = timer()
 
	# trunning json data recieved from " "  into python objects
	newstream1 = data_stream1.map(lambda recieved: json.loads(recieved))
	# # trunning json data recieved from " "  into python objects
	newstream2 = data_stream2.map(lambda recieved: json.loads(recieved))
	# # trunning json data recieved from " "  into python objects
	# newstream3 = data_stream3.map(lambda recieved: json.loads(recieved))
	#combining all the stream
	# combinestream = newstream1.union(newstream2).union(newstream3)
	combinestream = newstream1.union(newstream2)
	# combinestream.foreachRDD(data_stream2)
	# print(data_stream2)

#-------------------------location----------------------------------------

############## UNUSED

	# #--------------location----------------

	# hash_location_cate = combinestream.map(get_hash_location_category)
	# hash_location_cate_index = hash_location_cate.flatMap(lambda xs:[(x,1) for x in xs])
	# # hash_location_cate_index.foreachRDD(process1)

	# hash_location_cate_tid = combinestream.map(get_hash_location_category_tid)
	# flat_hash_location_cate_tid = hash_location_cate_tid.flatMap(lambda xs:[(x[0], x[1]) for x in xs if x!=[]])
	# # flat_hash_location_cate_tid.foreachRDD(process2)

	# user_location_cate = combinestream.map(get_location_with_user_category)
	# user_location_cate_index = user_location_cate.flatMap(lambda xs:[(x,1) for x in xs])
	# # user_location_cate_index.foreachRDD(process3)

	# user_location_cate_tid = combinestream.map(get_location_with_user_category_tweet_id)
	# flat_location_user_cate_tid = user_location_cate_tid.flatMap(lambda xs:[(x[0], x[1]) for x in xs if x!=[]])
	# # flat_location_user_cate_tid.foreachRDD(process4)
	# # combinestream.foreachRDD(process4)

	# keyword_location_cate = combinestream.map(get_keyword_location_category)
	# keyword_location_cate_index = keyword_location_cate.flatMap(lambda xs:[(x,1) for x in xs])
	# # keyword_location_cate_index.foreachRDD(process1)

	# keyword_location_cate_tid = combinestream.map(get_keyword_location_category_tweetid)
	# flat_keyword_location_cate_tid = keyword_location_cate_tid.flatMap(lambda xs:[(x[0], x[1]) for x in xs if x!=[]])
	# # flat_keyword_location_cate_tid.foreachRDD(process2)
	# men_location_cate = combinestream.map(get_men_location_category)
	# men_location_cate_index = men_location_cate.flatMap(lambda xs:[(x,1) for x in xs])
	# # men_location_cate_index.foreachRDD(process3)

	# men_location_cate_tid = combinestream.map(get_men_location_category_tid)
	# flat_men_location_cate_tid = men_location_cate_tid.flatMap(lambda xs:[(x[0], x[1]) for x in xs if x!=[]])
	# # flat_men_location_cate_tid.foreachRDD(process4)
	# #------------------------------------------------------------------------------------------------

#-------------------------------location end------------------------------



# ######################-------------------------------token-----------------------#################################

	
	category_with_location=combinestream.map(get_category_location)
	# category_with_location.foreachRDD(process1)
	category_with_location_index = category_with_location.flatMap(lambda xs: [(x, 1) for x in xs])
	# category_with_location_index.foreachRDD(process1)
	tweet_id_list_category_with_location = combinestream.map(get_category_location_tidlist)
	combine_tweet_id_list_category_with_location = tweet_id_list_category_with_location.flatMap(lambda xs:[(x[0], x[1]) for x in xs if x!=[]])
	# combine_tweet_id_list_category_with_location.foreachRDD(process2)

	category_with_hashtag=combinestream.map(get_category_hashtag)
	category_with_hashtag_index = category_with_hashtag.flatMap(lambda xs: [(x, 1) for x in xs])
	# category_with_hashtag_index.foreachRDD(process1)
	tweet_id_list_category_with_hashtag = combinestream.map(get_category_hashtag_tidlist)
	combine_tweet_id_list_category_with_hashtag = tweet_id_list_category_with_hashtag.flatMap(lambda xs:[(x[0], x[1]) for x in xs if x!=[]])

	category_keyword = combinestream.map(get_category_keyword)
	category_keyword_index = category_keyword.flatMap(lambda xs: [(x, 1) for x in xs])
	# category_keyword_index.foreachRDD(process1)
	category_key_tidlist = combinestream.map(get_category_keyword_tweetidlist)
	combine_tid_list_category_key=category_key_tidlist.flatMap(lambda xs:[(x[0], x[1]) for x in xs if x!=[]])
	# combine_tid_list_category_key.foreachRDD(process2)

	category_user = combinestream.map(get_user_category)
	category_user_index = category_user.flatMap(lambda xs: [(x, 1) for x in xs])
	category_user_tidlist = combinestream.map(get_user_category_tweetid)
	combine_tweet_id_user_category_list = category_user_tidlist.flatMap(lambda xs:[(x[0], x[1]) for x in xs if x!=[]])

	category_mention = combinestream.map(get_category_mention)
	category_mention_index = category_mention.flatMap(lambda xs: [(x, 1) for x in xs])
	# category_mention_index.foreachRDD(process1)
	category_mention_tidlist = combinestream.map(get_category_mention_tweetidlist)
	combine_tweet_id_mention_category_list = category_mention_tidlist.flatMap(lambda xs:[(x[0], x[1]) for x in xs if x!=[]])
	# combine_tweet_id_mention_category_list.foreachRDD(process2)
	
	union_ch_ck_cu_cm = category_with_hashtag_index.union(category_keyword_index).union(category_user_index).union(category_mention_index)\
	.union(category_with_location_index)
	union_chtid_cktid_cutid_cmtid = combine_tweet_id_list_category_with_hashtag.union(combine_tid_list_category_key).union(combine_tweet_id_user_category_list).union(combine_tweet_id_mention_category_list)\
	.union(combine_tweet_id_list_category_with_location)

	category_count=union_ch_ck_cu_cm.reduceByKey(lambda a,b:(a+b))
	# category_count.foreachRDD(process1)
	remap_category_count = category_count.map(get_remap_data)
	combine_category_count = remap_category_count.reduceByKey(lambda a,b:(a,b))

	category_tidlist=union_chtid_cktid_cutid_cmtid.reduceByKey(lambda a,b:(a,b))
	remap_category_tidlist=category_tidlist.map(get_remap_data)
	combine_category_tidlist = remap_category_tidlist.reduceByKey(lambda a,b:(a,b))

	combine_h_c_t =combine_category_count.union(combine_category_tidlist)
	reduce_h_c_t = combine_h_c_t.reduceByKey(lambda a,b:(a,b))
	# reduce_h_c_t.foreachRDD(process1)
	final_h_c_t = reduce_h_c_t.map(data_formator)
	# final_h_c_t.foreachRDD(process2)
	final_h_c_t.foreachRDD(store_token_category_data)




	#hashtag-hashtag-category
	# hash_hash_cate = combinestream.map(get_hash_hash_category)
	# hash_hash_cate_index = hash_hash_cate.flatMap(lambda xs:[(x,1) for x in xs])
	# hash_hash_cate_count = hash_hash_cate_index.reduceByKey(lambda a,b:(a+b))
	# remap_hash_hash_cate_count=hash_hash_cate_count.map(get_remap_data)
	# # remap_hash_hash_cate_count.foreachRDD(process1)
	# hash_hash_cate_tid = combinestream.map(get_hash_hash_category_tid)
	# flat_hash_hash_cate_tid = hash_hash_cate_tid.flatMap(lambda xs:[(x[0], x[1]) for x in xs if x!=[]])
	# combine_hash_hash_cate_tid = flat_hash_hash_cate_tid.reduceByKey(lambda a,b:(a,b))
	# remap_combine_hash_hash_cate_tid = combine_hash_hash_cate_tid.map(get_remap_data)
	# # remap_combine_hash_hash_cate_tid.foreachRDD(process2)
	# union_h_tid = remap_hash_hash_cate_count.union(remap_combine_hash_hash_cate_tid)
	# reduce_h_tid = union_h_tid.reduceByKey(lambda a,b:(b,a))
	# reduce_h_tid.foreachRDD(process2)
	hash_hash_cate = combinestream.map(get_hash_hash_category)
	hash_hash_cate_index = hash_hash_cate.flatMap(lambda xs:[(x,1) for x in xs])
	# hash_hash_cate_index.foreachRDD(process1)

	hash_hash_cate_tid = combinestream.map(get_hash_hash_category_tid)
	flat_hash_hash_cate_tid = hash_hash_cate_tid.flatMap(lambda xs:[(x[0], x[1]) for x in xs if x!=[]])

	hash_men_cate = combinestream.map(get_hash_mention_category)
	hash_men_cate_index = hash_men_cate.flatMap(lambda xs:[(x,1) for x in xs])
	# hash_men_cate_index.foreachRDD(process1)

	hash_men_cate_tid = combinestream.map(get_hash_mention_category_tid)
	flat_hash_men_cate_tid = hash_men_cate_tid.flatMap(lambda xs:[(x[0], x[1]) for x in xs if x!=[]])
	# flat_hash_men_cate_tid.foreachRDD(process2)

	hash_user_cate = combinestream.map(get_hastag_with_user_category)
	hash_user_cate_index = hash_user_cate.flatMap(lambda xs:[(x,1) for x in xs])
	# hash_user_cate_index.foreachRDD(process1)

	hash_user_cate_tid = combinestream.map(get_hastag_with_user_category_tweet_id)
	flat_hash_user_cate_tid = hash_user_cate_tid.flatMap(lambda xs:[(x[0], x[1]) for x in xs if x!=[]])
	# flat_hash_user_cate_tid.foreachRDD(process2)
	# combinestream.foreachRDD(process4)

	hash_keyword_cate = combinestream.map(get_keyword_hashtag_category)
	hash_keyword_cate_index = hash_keyword_cate.flatMap(lambda xs:[(x,1) for x in xs])
	# hash_keyword_cate_index.foreachRDD(process1)

	hash_keyword_cate_tid = combinestream.map(get_keyword_hashtag_category_tweetid)
	flat_hash_keyword_cate_tid = hash_keyword_cate_tid.flatMap(lambda xs:[(x[0], x[1]) for x in xs if x!=[]])
	# flat_hash_keyword_cate_tid.foreachRDD(process2)

	men_men_cate = combinestream.map(get_mention_mention_category)
	men_men_cate_index = men_men_cate.flatMap(lambda xs:[(x,1) for x in xs])
	# men_men_cate_index.foreachRDD(process1)

	men_men_cate_tid = combinestream.map(get_mention_mention_tid)
	flat_men_men_cate_tid = men_men_cate_tid.flatMap(lambda xs:[(x[0], x[1]) for x in xs if x!=[]])
	# flat_men_men_cate_tid.foreachRDD(process2)

	men_user_cate = combinestream.map(get_mention_user_category)
	men_user_cate_index = men_user_cate.flatMap(lambda xs:[(x,1) for x in xs])
	# men_user_cate_index.foreachRDD(process1)

	men_user_cate_tid = combinestream.map(get_mention_with_user_category_tweet_id)
	flat_men_user_cate_tid = men_user_cate_tid.flatMap(lambda xs:[(x[0], x[1]) for x in xs if x!=[]])
	# flat_men_user_cate_tid.foreachRDD(process2)

	men_keyword_cate = combinestream.map(get_mention_keyword_category)
	men_keyword_cate_index = men_keyword_cate.flatMap(lambda xs:[(x,1) for x in xs])

	men_keyword_cate_tid = combinestream.map(get_mention_keyword_category_tweetid)
	flat_men_keyword_cate_tid = men_keyword_cate_tid.flatMap(lambda xs:[(x[0], x[1]) for x in xs if x!=[]])

	key_key_cate = combinestream.map(get_key_key_category)
	key_key_cate_index = key_key_cate.flatMap(lambda xs:[(x,1) for x in xs])

	key_key_cate_tid = combinestream.map(get_key_key_category_tid)
	flat_key_key_cate_tid = key_key_cate_tid.flatMap(lambda xs:[(x[0], x[1]) for x in xs if x!=[]])

	key_user_cate=combinestream.map(get_keyword_with_user_category)
	key_user_cate_index = key_user_cate.flatMap(lambda xs:[(x,1) for x in xs])
	# key_user_cate_index.foreachRDD(process1)

	key_user_cate_tid = combinestream.map(get_keyword_with_user_category_tweet_id)
	flat_key_user_cate_tid = key_user_cate_tid.flatMap(lambda xs:[(x[0], x[1]) for x in xs if x!=[]])
	# flat_key_user_cate_tid.foreachRDD(process2)


	union_category_token_co = hash_hash_cate_index.union(hash_men_cate_index).union(hash_user_cate_index).\
	union(hash_keyword_cate_index).union(men_men_cate_index).union(men_user_cate_index).union(men_keyword_cate_index).union(key_key_cate_index).\
	union(key_user_cate_index)
	
	union_category_token_co_tid = flat_hash_hash_cate_tid.union(flat_hash_men_cate_tid).union(flat_hash_user_cate_tid).union(flat_hash_keyword_cate_tid).union(flat_men_men_cate_tid).union(flat_men_user_cate_tid).\
	union(flat_men_keyword_cate_tid).union(flat_key_key_cate_tid).union(flat_key_user_cate_tid)
	
	category_co_occ_count=union_category_token_co.reduceByKey(lambda a,b:(a+b))
	remap_category_co_occ_count = category_co_occ_count.map(get_remap_data)
	combine_category_co_occ_count = remap_category_co_occ_count.reduceByKey(lambda a,b:(a,b))

	category_co_occ_tidlist=union_category_token_co_tid.reduceByKey(lambda a,b:(a,b))
	remap_category_co_occ_tidlist=category_co_occ_tidlist.map(get_remap_data)
	combine_category_co_occ_tidlist = remap_category_co_occ_tidlist.reduceByKey(lambda a,b:(a,b))

	combine_co_occurence =combine_category_co_occ_count.union(combine_category_co_occ_tidlist)
	reduce_co_occurence = combine_co_occurence.reduceByKey(lambda a,b:(a,b))
	# reduce_co_occurence.foreachRDD(process1)
	final_co_occurece = reduce_co_occurence.map(data_formator)
	# final_co_occurece.foreachRDD(process2)
	final_co_occurece.foreachRDD(store_co_occurence_token_category_data)


#################-----------------------------loaction-with-place-----------------------------################

	#hashtag location

	location_with_hashtag_place = combinestream.map(get_cat_location_hashtag_place)
	flatten_Location_with_hashtag_place = location_with_hashtag_place.flatMap(lambda xs: [(x, 1) for x in xs])

	tweet_id_location_hashtag_list_place = combinestream.map(get_cat_hashtag_with_location_tweet_id_place)
	combine_tweet_id_hashtag_location_list_place = tweet_id_location_hashtag_list_place.flatMap(lambda xs:[(x[0], x[1]) for x in xs if x!=[]])

	#keyword location

	location_with_keyword_place = combinestream.map(get_cat_location_keyword_place)
	flatten_location_with_keyword_place=location_with_keyword_place.flatMap(lambda xs: [(x, 1) for x in xs])

	tweet_id_location_keyword_list_place = combinestream.map(get_cat_keyword_with_location_tweet_id_place)
	combine_tweet_id_keyword_location_list_place = tweet_id_location_keyword_list_place.flatMap(lambda xs:[(x[0], x[1]) for x in xs if x!=[]])

	#user location

	location_user_place = combinestream.map(get_cat_location_user)
	flatten_location_with_user_place = location_user_place.flatMap(lambda xs: [(x, 1) for x in xs])

	tweet_id_location_user_list_place = combinestream.map(get_cat_user_location_tweet_id)
	combine_tweet_id_user_location_list_place = tweet_id_location_user_list_place.flatMap(lambda xs:[(x[0], x[1]) for x in xs if x!=[]])

	#mention location
	
	mention_location_place = combinestream.map(get_cat_mention_location_place)
	flatten_location_with_mention_place=mention_location_place.flatMap(lambda xs: [(x, 1) for x in xs])

	tweet_id_location_mention_list_place = combinestream.map(get_cat_mention_with_location_tweet_id_place)
	combine_tweet_id_mention_location_list_place = tweet_id_location_mention_list_place.flatMap(lambda xs:[(x[0], x[1]) for x in xs if x!=[]])

	#operations

	union_location_token = flatten_Location_with_hashtag_place.union(flatten_location_with_keyword_place).union(flatten_location_with_user_place).union(flatten_location_with_mention_place)

	union_location_token_tid = combine_tweet_id_hashtag_location_list_place.union(combine_tweet_id_keyword_location_list_place).union(combine_tweet_id_user_location_list_place).union(combine_tweet_id_mention_location_list_place)

	union_location_token_count = union_location_token.reduceByKey(lambda a,b:(a+b))
	# union_location_token_count.foreachRDD(process2)
	# remap_union_location_token_count = union_location_token_count.map(get_remap_data)
	# combine_category_union_token = remap_union_location_token_count.reduceByKey(lambda a,b:(a,b))

	union_location_token_count_tid = union_location_token_tid.reduceByKey(lambda a,b:(a+b))
	# union_location_token_count_tid.foreachRDD(process3)
	# remap_union_location_token_count_tid = union_location_token_count_tid.map(get_remap_data)
	# combine_category_union_token_tid = remap_union_location_token_count_tid.reduceByKey(lambda a,b:(a,b))

	combine_location_token_tid = union_location_token_count.union(union_location_token_count_tid)
	reduce_location_token_tid = combine_location_token_tid.reduceByKey(lambda a,b:(a,b))
	# reduce_location_token_tid.foreachRDD(process1)

	final_location_token_tid = reduce_location_token_tid.map(data_formator_location)
	final_location_token_tid.foreachRDD(store_token_category_location)




###############################################################


	end_time = timer()
	print("This is the start time",start_time)
	print("This is the end time",end_time)
	time_elapsed = start_time-end_time
	print("Time elpsed is",time_elapsed)
	ssc.start()
	ssc.awaitTermination()


		
if __name__=='__main__':

	main()
	


	






