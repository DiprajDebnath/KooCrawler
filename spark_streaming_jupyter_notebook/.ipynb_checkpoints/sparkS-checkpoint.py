# Importing sparkContext & StreamingContext from PySpark library
#from _future_ import print_function
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import json
# Creating sparkContext with AppName abc
sc = SparkContext(appName="abc")
sc.setLogLevel("ERROR")

#Creating Spark Streaming Context using SC with patameter 10s batch interval
ssc = StreamingContext(sc, 10)

#Connecting to crowler 
socket_stream = ssc.socketTextStream("172.16.117.15", 9013)
socket_stream.pprint()

# analysis on tweets collected in 20 secs
lines = socket_stream.window(30)
lines.pprint()

# stored tweet in lines -> split message into words using flatMap -> filter hashtag -> convert into lowercase -> 
hashtags= lines.flatMap(lambda text: text.split(" ")).filter(lambda word: word.lower().startswith('#')).map( lambda word:( word.lower(),1)).reduceByKey(lambda a,b:a+b)

# Split each line into words
words = lines.flatMap(lambda line: line.split(" "))
words.pprint()

hashtags = words.filter(lambda word: word.lower().startswith('#'))
hashtags.pprint()


# mentions = words.filter(lambda word: word.lower().startswith('@'))
# mentions.pprint()

# # Count each word in each batch
# pairsHash = hashtags.map( lambda word:( word.lower(),1))
# pairsHash.pprint()
# hashtagsCounts = pairsHash.reduceByKey(lambda a,b:a+b)
# hashtagsCounts.pprint()

# pairsMention = mentions.map(lambda word:(word.lower(),1))
# pairsMention.pprint()
# mentionsCounts = pairsMention.reduceByKey(lambda a, b:a+b)
# mentionsCounts.pprint()

# union = hashtagsCounts.union(mentionsCounts)
# union.pprint()


# author_counts_sorted = hashtags.transform(lambda foo:foo.sortBy(lambda x:x[0].lower()).sortBy(lambda x:x[1], ascending=False))
# author_counts_sorted.pprint()
ssc.start()
ssc.awaitTermination()
