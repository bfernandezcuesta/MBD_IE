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
        row_rdd = rdd.map(lambda w: Row(word=w[0], word_count=w[1]))
        words_df = sql_context.createDataFrame(row_rdd)
        words_df.registerTempTable("words_table")
        word_counts_df = sql_context.sql("select word, word_count from words_table order by word_count desc limit 10")
        word_counts_df.show()
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


# map each word to be a pair of (word, 1)
pairs = words.map(lambda word: (word, 1))


# adding the count of each word to its last count 
word_count = pairs.reduceByKeyAndWindow(lambda x, y: int(x) + int(y), lambda x, y: int(x) - int(y), 600, 30)

word_count.pprint()

# do the processing for each RDD generated in each interval
word_count.foreachRDD(process_rdd)


# start the streaming computation
ssc.start()


# wait for the streaming to finish
ssc.awaitTermination()



### TOP 10 ELEMENTS -- OUTPUT -- WORD COUNT -- tracked word: "fase"



#   -------------------------------------------
#   Time: 2020-05-15 15:32:04
#   -------------------------------------------
#   ('este', 1)
#   ('ritmo,', 1)
#   ('hasta', 2)
#   ('septiembre', 1)
#   ('no', 20)
#   ('fase', 68)
#   ('si', 7)
#   ('barrio', 1)
#   ('pasado', 2)
#   ('@carkrasheart:', 2)
#   ...

#   ----------- 2020-05-15 15:32:04 -----------
#   +----+----------+
#   |word|word_count|
#   +----+----------+
#   |  la|        73|
#   |fase|        68|
#   |  RT|        60|
#   |   a|        60|
#   |  de|        53|
#   | que|        37|
#   |Fase|        29|
#   |   1|        23|
#   |  no|        20|
#   |  en|        19|
#   +----+----------+
