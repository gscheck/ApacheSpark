from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
import pyspark.sql.types as SQLTypes
from pyspark.sql import Row
from pyspark.sql import Column as col
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoderEstimator, VectorAssembler
from pyspark.ml.feature import StopWordsRemover, Word2Vec, RegexTokenizer
from pyspark.ml.classification import LogisticRegression
import requests
import matplotlib.pyplot as plt
import pandas as pd
import sys
import pyspark.sql.functions as f
import matplotlib.ticker as ticker
import matplotlib.animation as animation
from IPython.display import HTML
from pandas.io import sql
import MySQLdb
import pymysql
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import create_engine


def getSparkSessionInstance(sparkConf):
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()
    return globals()["sparkSessionSingletonInstance"]

def process(rdd):
    try:
        spark = getSparkSessionInstance(rdd.context.getConf())
        # remove the blank tweets
        tweet_text = rdd.filter(lambda x: len(x) > 0)
        # create the dataframe with each row contains a tweet text
        rowRdd = tweet_text.map(lambda w: Row(tweet=w))
        
        
        wordsDataFrame = spark.createDataFrame(rowRdd)
        
        wordsDataFrame.foreach(print)
        
      
        # Define the schema of our dataset
        my_schema = SQLTypes.StructType([
            SQLTypes.StructField(name='id', dataType=SQLTypes.IntegerType(), nullable=True),
            SQLTypes.StructField(name='label', dataType=SQLTypes.IntegerType(), nullable=True),
            SQLTypes.StructField(name='tweet', dataType=SQLTypes.StringType(), nullable=True)
        ])
        
        # Load the csv
        my_data = spark.read.csv('d:/temp/Twitter_HateSpeech.csv', schema=my_schema, header=True)
        
        # For visualizing
        my_data.show(5)    

        # Define our ML pipeline, which we will pass streamed tweets through to generate a prediction
        
        # Stage 1: Tokenize the tweet
        stage1 = RegexTokenizer(inputCol = 'tweet', outputCol = 'tokens', pattern = '\\W')
        
        # Stage 2: Remove stop words
        stage2 = StopWordsRemover(inputCol = 'tokens', outputCol = 'filtered_words')
        
        # Stage 3: Create a word vector (size 100 will do, since tweets can only contain 140 characters max)
        stage3 = Word2Vec(inputCol = 'filtered_words', outputCol = 'vector', vectorSize = 100)
        
        # Stage 4: Pass the data into a logistic regression model
        model = LogisticRegression(featuresCol = 'vector', labelCol = 'label')
    
        # Assemble the pipleline
        pipeline = Pipeline(stages = [stage1, stage2, stage3, model])
        
        pipelineFit = pipeline.fit(my_data)          
        
        # for debug
        #print("show transform")
        #pipelineFit.transform(wordsDataFrame).select('tweet','prediction').show()
        
        # get the sentiments for each row
        df = pipelineFit.transform(wordsDataFrame).select('tweet','prediction')
        send_df_to_dashboard(df)
    except : 
        print('No data')

def processWords(time, rdd):
    print("========= %s =========" % str(time))
    try:
        print("get spark session")	
        # Get the singleton instance of SparkSession
        spark = getSparkSessionInstance(rdd.context.getConf())

        print("convert rdd")	
        # Convert RDD[String] to RDD[Row] 
        rowRdd = rdd.map(lambda w: Row(word=w))

        print("create df")   
        # Create dataframe from Row
        wordsDataFrame = spark.createDataFrame(rowRdd)

        # Creates a temporary view using the DataFrame
        # Grouping by word to get counts
        print("group by words to get counts")
        wf = wordsDataFrame.groupBy('word').count()

        print("register temp table")
        # Create a temp table "words" to perform SQL against
        wf.registerTempTable("words")

        print("select values")
        # Select word and number of occurances from temp table
        df = spark.sql("SELECT word, count as wd_count FROM words ORDER BY wd_count DESC limit 100")

        # filter out unwanted sequence of characters
        df=df.filter(~df.word.like('[%') & ~df.word.like('{%') & ~df.word.like(''))

        # for debugging
        df.show()

        # send to dashboard was originally going to post to a WEB page
        # right now we have it writing to a MySQL database to be consumed by a C# application
        send_wds_to_dashboard(df)
    except Exception as e: print(e)
    
def send_wds_to_dashboard(df):
    print("send data frame to dashboard")
    # extract the hashtags from dataframe and convert them into array
    print("get top words")
    top_words = [str(t.word) for t in df.select("word").collect()]

    print("get word counts")
    word_count = [p.wd_count for p in df.select("wd_count").collect()]

    #url = 'http://localhost:5001/updateData'
    print("create array")
    request_data = {'label': str(top_words), 'data': str(word_count)}
    
    print(request_data)
    print("to pandas df")
    pandas_df = df.toPandas()
    
    # create connection to MySQL database
    con = create_engine('mysql+pymysql://spark:spark@127.0.0.1/twitter')
 
    # append records to table
    pandas_df.to_sql('word_count_t', con=con, if_exists='append', index = False)
    
    # close connection to database
    con.discard()

def send_df_to_dashboard(df):
    print("send data frame to dashboard")
     
    print("spark df to pandas df")
    pandas_df = df.toPandas()
    
    # create connection to MySQL database
    con = create_engine('mysql+pymysql://spark:spark@127.0.0.1/twitter')
 
    # append records to table
    pandas_df.to_sql('tweet_t', con=con, if_exists='append', index = False)
    
    # close connection to database
    con.discard()

if __name__ == "__main__":

    TCP_IP = "localhost"
    TCP_PORT = 9009
    
    application_name = "TwitterStreamApp"

    master = "spark://DESKTOP-63IVCLC.localdomain:7077"
    num_executors = 4
    num_cores = 2

    
    # create spark configuration
    conf = SparkConf()
    conf.setAppName("TwitterStreamApp")
    
    # create spark context with the above configuration
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)
    sc.setLogLevel("ERROR")
  
   
    # create the Streaming Context from the above spark context with interval size 1 second
    ssc = StreamingContext(sc, 1)
       
    # setting a checkpoint to allow RDD recovery
    ssc.checkpoint("checkpoint_TwitterApp")
    
    # read data from port 9009 and split into words
    #lines = ssc.socketTextStream(TCP_IP, TCP_PORT).flatMap(lambda line: line.split(' '))
    
    gen = ssc.socketTextStream(TCP_IP, TCP_PORT)
    
    #lines = ssc.socketTextStream(TCP_IP, TCP_PORT).flatMap(lambda line: line.split('TWEET_APP'))

    lines = gen.flatMap(lambda line: line.split('TWEET_APP'))
    words = gen.flatMap(lambda line: line.split(' '))

    #process words
    lines.foreachRDD(process)
    words.foreachRDD(processWords)
    
    # start the streaming
    ssc.start()
    
    # wait for the streaming to finish
    ssc.awaitTermination()
