# import os
# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 pyspark-shell'
# import findspark
# findspark.init('/Users/lorenapersonel/Downloads/spark-3.2.1-bin-hadoop3.2-scala2.13')

from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.functions import split
from pyspark.sql.types import StringType, StructType, StructField, FloatType
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
from pyspark.ml.feature import RegexTokenizer
import re
from textblob import TextBlob
import configparser
from datetime import datetime
config = configparser.ConfigParser()
config.read('/home/bdm/tweet/data/twitter-app-credentials.txt')
mongo_url = config['MONGO']['mongo_url_stream']

date_str = datetime.now().strftime("%Y_%m_%d-%H:%M")

date = datetime.strptime(date_str, "%Y_%m_%d-%H:%M")

# remove_links
def cleanTweet(tweet: str) -> str:
    tweet = re.sub(r'http\S+', '', str(tweet))
    tweet = re.sub(r'bit.ly/\S+', '', str(tweet))
    tweet = tweet.strip('[link]')

    # remove users
    tweet = re.sub('(RT\s@[A-Za-z]+[A-Za-z0-9-_]+)', '', str(tweet))
    tweet = re.sub('(@[A-Za-z]+[A-Za-z0-9-_]+)', '', str(tweet))

    # remove puntuation
    my_punctuation = '!"$%&\'()*+,-./:;<=>?[\\]^_`{|}~•@â'
    tweet = re.sub('[' + my_punctuation + ']+', ' ', str(tweet))

    # remove number
    tweet = re.sub('([0-9]+)', '', str(tweet))

    # remove hashtag
    tweet = re.sub('(#[A-Za-z]+[A-Za-z0-9-_]+)', '', str(tweet))

    return tweet[10:]


# Create a function to get the subjectifvity
def getSubjectivity(tweet: str) -> float:
    return TextBlob(tweet).sentiment.subjectivity


# Create a function to get the polarity
def getPolarity(tweet: str) -> float:
    return TextBlob(tweet).sentiment.polarity


def getSentiment(polarityValue: int) -> str:
    if polarityValue < 0:
        return 'Negative'
    elif polarityValue == 0:
        return 'Neutral'
    else:
        return 'Positive'


def getCompany(news: str)-> str:
    with open('/home/bdm/tweet/data/company_list.txt') as f:
        tmp_list = f.readlines()

    company_list = []
    for  i in range(len(tmp_list)):
        company_list.append(tmp_list[i].strip())

    for i in company_list:
        if i in news:
            return i
    
    return "others"


# epoch
def write_row_in_mongo(df,epoch_id):
    df.write.format("mongo").mode("append").option("uri", mongo_url).save()
    pass



if __name__ == "__main__":

    spark = SparkSession.builder.appName("twitter-sentiment") \
        .config("spark.mongodb.input.uri", mongo_url) \
        .config("spark.mongodb.output.uri", mongo_url) \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0") \
        .getOrCreate()


    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "stream-app") \
        .option("startingOffsets", "latest")\
        .load()

    schema = StructType([                                                                                          
    StructField("data", StructType([                                                                                          
    StructField("text", StringType(), True)   
    ]), True)   
    ])




    kafka_df_string = df.selectExpr("CAST(value AS STRING)","timestamp")

    tweet_df = kafka_df_string.select(from_json(col("value"), schema).alias("data"),"timestamp").select("data.*","timestamp")

    #tmp_df = tweet_df.select()
    clean_tweets = F.udf(cleanTweet, StringType())
    raw_tweets = tweet_df.withColumn('processed_text', clean_tweets(col("data")))
    # udf_stripDQ = udf(stripDQ, StringType())

    subjectivity = F.udf(getSubjectivity, FloatType())
    polarity = F.udf(getPolarity, FloatType())
    sentiment = F.udf(getSentiment, StringType())
    compname = F.udf(getCompany, StringType())

    subjectivity_tweets = raw_tweets.withColumn('subjectivity', subjectivity(col("processed_text")))
    polarity_tweets = subjectivity_tweets.withColumn("polarity", polarity(col("processed_text")))
    sentiment_tweets = polarity_tweets.withColumn("sentiment", sentiment(col("polarity")))
    company_tweets = sentiment_tweets.withColumn("company_name", compname(col("processed_text")))

    #selection_df=company_tweets.select("*").where("company_name != 'others'")   

    selection_df = company_tweets.filter(company_tweets.company_name != "others")
   
    #selection_df = company_tweets.filter(company_tweets.company_name == "others")

    # selection_df = selection_df.withColumn("data",create_map(
    #     lit("processed_text"),col("processed_text"),
    #     lit("subjectivity"),col("subjectivity"),
    #     lit("polarity"),col("polarity"),
    #     lit("sentiment"),col("sentiment")
    #     )).drop("subjectivity","polarity","sentiment")
    aggregated_df = selection_df.withWatermark("timestamp", "2 minutes").groupBy(window("timestamp", "2 minutes", "2 minutes"),"company_name").agg(
    F.collect_list(F.struct('processed_text','subjectivity','polarity','sentiment')).alias('data'))
    

    # query = aggregated_df.writeStream.format("console").start()
    # import time
    # time.sleep(3) # sleep 10 seconds
    # query.awaitTermination()




    query = aggregated_df.writeStream \
        .foreachBatch(write_row_in_mongo).start()
    query.awaitTermination()




    # query = selection_df.writeStream.format("console").start()
    # import time
    # time.sleep(3) # sleep 10 seconds
    # query.awaitTermination()
