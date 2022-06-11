import os
from jsonschema import draft4_format_checker
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import *
from pyspark.sql.functions import from_json, col, udf
from pyspark.ml.feature import RegexTokenizer
import re
from textblob import TextBlob
from pyspark.sql import SparkSession
import configparser
from datetime import datetime

#import pyspark.pandas as ps
#hdfs dfs -cat stock/news/2022_04_05/file_00.json


def clean_text(news: str) -> str:
    news = re.sub(r'http\S+', '', str(news))
    news = re.sub(r'bit.ly/\S+', '', str(news))
    news = news.strip('[link]')
    # remove puntuation
    my_punctuation = '!"$%&\'()*+,-./:;<=>?[\\]^_`{|}~•@â'
    news = re.sub('[' + my_punctuation + ']+', ' ', str(news))

    # remove number
    news = re.sub('([0-9]+)', '', str(news))

    return news

# Create a function to get the subjectifvity
def get_subjectivity(news: str) -> float:
    return TextBlob(news).sentiment.subjectivity


# Create a function to get the polarity
def get_polarity(news: str) -> float:
    return TextBlob(news).sentiment.polarity


def get_sentiment(polarityValue: int) -> str:
    if polarityValue < 0:
        return 'Negative'
    elif polarityValue == 0:
        return 'Neutral'
    else:
        return 'Positive'

def get_company(news: str)-> str:
    with open('/home/bdm/tweet/data/company_list.txt') as f:
        tmp_list = f.readlines()

    company_list = []
    for  i in tmp_list:
        company_list.append(i.strip())

    for i in company_list:
        if i in news:
            return i
    
    return "others"


date_str = datetime.now().strftime("%Y_%m_%d-%H")

date = datetime.strptime(date_str, "%Y_%m_%d-%H")

folder_name = '2022_04_05/'
file_name = 'file_03.json'



'''
ddate = date_str.split("-")
folder_name = ddate[0]+'/'
file_name = 'file_' + ddate[1] + '.json'
'''


config = configparser.ConfigParser()
config.read('/home/bdm/tweet/data/twitter-app-credentials.txt')
mongo_url = config['MONGO']['mongo_url']

spark = SparkSession.builder.appName("news-sentiment") \
    .config("spark.mongodb.input.uri", mongo_url) \
    .config("spark.mongodb.output.uri", mongo_url) \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .getOrCreate()

df = spark.read.json("hdfs://10.4.41.50:27000/user/bdm/stock/news/"+folder_name+file_name)

new_df = df.selectExpr('inline_outer(articles)')

#new_df.show()


# RDD Operations on news data
rdd1 = new_df.rdd
rdd2 = rdd1.map(lambda x : (x['title'],x['publishedAt'],x['source']['name']))
rdd3 = rdd2.map (lambda x : (x[0],x[1],x[2],clean_text(x[0])))

rdd4 = rdd3.map (lambda x : (x[0],x[1],x[2],get_subjectivity(x[3]),get_polarity(x[3])))
rdd5 = rdd4.map (lambda x : (x[0],x[1],x[2],x[3],x[4],get_sentiment(x[4])))


rdd6 = rdd5.map (lambda x : [date,x[0],x[1],x[2],x[3],x[4],x[5],get_company(x[0])])
rdd6.collect()


        

mongo_schema =  StructType([

    StructField('date', TimestampType(), True),
    StructField('title', StringType(), True),
    StructField('published_time', StringType(), True),
    StructField('source', StringType(), True),
    StructField('subjectivity', FloatType(), True),
    StructField('polarity', FloatType(), True),
    StructField('sentiment', StringType(), True),
    StructField('company_name', StringType(), True),
     
              ])


processed_df = rdd6.toDF(mongo_schema)

aggregated_df = processed_df.groupBy("date","company_name").agg(
    F.collect_list(F.struct('title', 'published_time','source','subjectivity','polarity','sentiment')).alias('news'))

final_df = aggregated_df.groupBy("date").agg(F.collect_list(F.struct('company_name', 'news')).alias('data'))
final_df.show()


final_df.write.format("mongo").mode("append").option("uri", mongo_url).save()

#***********************************************************************#

