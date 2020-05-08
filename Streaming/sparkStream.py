from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import desc
from collections import namedtuple
import time
from IPython import display
import matplotlib.pyplot as plt
from ggplot import *

sc = SparkContext()

ssc = StreamingContext(sc, 10 )
sqlContext = SQLContext(sc)
socket_stream = ssc.socketTextStream("localhost", 5555)
lines = socket_stream.window( 20 )
fields = ("tag", "count" )
Tweet = namedtuple( 'Tweet', fields )

( lines.flatMap( lambda text: text.split( " " ) )
.filter( lambda word: word.lower().startswith("#") )
.map( lambda word: ( word.lower(), 1 ) )
.reduceByKey( lambda a, b: a + b )
.map( lambda rec: Tweet( rec[0], rec[1] ) )
.foreachRDD( lambda rdd: rdd.toDF().sort( desc("count") )
.limit(10).registerTempTable("tweets") ) )

ssc.start()

count = 0
while count < 10:
    time.sleep( 3 )
    top_10_tweets = sqlContext.sql( 'Select tag, count from tweets' )
    top_10_df = top_10_tweets.toPandas()
    display.clear_output(wait=True)
    ggplot(aes(x="x", weight="y"), top_10_df) + geom_bar()
	
    count = count + 1