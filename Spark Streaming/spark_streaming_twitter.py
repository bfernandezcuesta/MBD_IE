from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests

def aggregate_tags_count(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)

def get_sql_context_instance(spark_context):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
    return globals()['sqlContextSingletonInstance']

def process_rdd(time, rdd):
    print("----------- %s -----------" % str(time))
    try:
        sql_context = get_sql_context_instance(rdd.context)
        row_rdd = rdd.map(lambda w: Row(hashtag=w[0], hashtag_count=w[1]))
        hashtags_df = sql_context.createDataFrame(row_rdd)
        hashtags_df.registerTempTable("hashtags")
        hashtag_counts_df = sql_context.sql("select hashtag, hashtag_count from hashtags order by hashtag_count desc limit 10")
        hashtag_counts_df.show()
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)


# create spark configuration
conf = SparkConf()
conf.setAppName("TwitterStreamApp")


# create spark context with the above configuration
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")


# create the Streaming Context from the above spark context with interval size 2 seconds
ssc = StreamingContext(sc, 2)


# setting a checkpoint to allow RDD recovery
ssc.checkpoint("blanca_checkpoint")


# read data from the port
tweet = ssc.socketTextStream("localhost", 7777)


# split each tweet into words
words = tweet.flatMap(lambda line: line.split(" "))


# filter the words to get only hashtags, then map each hashtag to be a pair of (hashtag,1)
hashtags = words.filter(lambda w: '#' in w).map(lambda x: (x, 1))


# adding the count of each hashtag to its last count using updateStateByKey
tags_totals = hashtags.updateStateByKey(aggregate_tags_count)

tags_totals.pprint()

# do the processing for each RDD generated in each interval
tags_totals.foreachRDD(process_rdd)


# start the streaming computation
ssc.start()


# wait for the streaming to finish
ssc.awaitTermination()



### TOP 10 ELEMENTS -- OUTPUT -- HASHTAG COUNT -- tracked word: "fase"


#   -------------------------------------------
#   Time: 2020-05-15 17:01:28
#   -------------------------------------------
#   ('#15Mayo', 2)
#   ('#albania', 1)
#   ('#DesescaladaMadrid,', 1)
#   ('#FIFA', 1)
#   ('#EstoyEnTwitch', 1)
#   ('#Covid19Ec:', 2)
#   ('#vacunas', 1)
#   ('#HastaLosHuevos', 1)
#   ('#doblevarademedir', 1)
#   ('#MaratonESC2019', 4)
#   ...
#   ----------- 2020-05-15 17:01:28 -----------
#   +-------------------+-------------+
#   |            hashtag|hashtag_count|
#   +-------------------+-------------+
#   |             #Gala2|            6|
#   |            #Madrid|            5|
#   |    #MaratonESC2019|            4|
#   |#IzquierdaMiserable|            3|
#   |        #Covid19Ec:|            2|
#   |            #15Mayo|            2|
#   |            #fase05|            2|
#   |          #15deMayo|            2|
#   |   #AnaRosaQuintana|            2|
#   |           #vacunas|            1|
#   +-------------------+-------------+
