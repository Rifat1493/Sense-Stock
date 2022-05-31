from hdfs import InsecureClient
from hdfs import Config
from hdfs.ext.avro import AvroWriter

from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType, StructType, StructField, StringType, IntegerType, DoubleType

import argparse
import pandas as pd
from datetime import datetime
from os import listdir
from os.path import join

# Instantiate the parser
parser = argparse.ArgumentParser(description="Stores files in current date's directory in HDFS")

# Required positional argument
parser.add_argument('--prod', action='store_true',
                    help='Flag that tells whether script is running on dev machine or prod upc-vm server')
args = parser.parse_args()

print(f"File Run: {datetime.now()}")

spark = SparkSession.builder \
    .appName("ABC app name") \
    .master("local[1]") \
    .getOrCreate()

todays_date_str = datetime.now().strftime("%Y-%m-%d")
base_dir = join("stock", "ohlc")
data_dir = join(base_dir, todays_date_str)
hdfs_save_dir = "stock"

schema = StructType([
    StructField('Datetime', TimestampType(), True),
    StructField("Open", DoubleType(), True),
    StructField("High", DoubleType(), True),
    StructField("Low", DoubleType(), True),
    StructField("Close", DoubleType(), True),
    StructField("Adj_Close", DoubleType(), True),
    StructField("Volume", IntegerType(), True)
])

# Writing using pySpark with parquet format
for file in listdir(data_dir):
    print(f"Saving file: {file}")

    # Writing using the Avro format (If file name on HDFS exists it gives "Connection reset by peer" )
    pandasDF = pd.read_csv(join(data_dir, file), parse_dates=["Datetime"])
    pandasDF = pandasDF.rename(columns={"Adj Close": "Adj_Close"})  # Remove spaces from column names

    sparkDF = spark.createDataFrame(pandasDF, schema=schema)

    # Create RDD from sparkDF
    rdd = sparkDF.rdd

    #  ------------- Do processing --------------- #
    # # # remove duplicate rows
    # make datetime the key
    rdd2 = rdd.map(lambda x: (x[0], (x[1], x[2], x[3], x[4], x[5], x[6])))
    # reduce by datetime thereby removing duplicates
    rdd3 = rdd2.reduceByKey(lambda a, b: a)
    # restore the tabular structure of the RDD
    rdd4 = rdd3.map(lambda x: (x[0], x[1][0], x[1][1], x[1][2], x[1][3], x[1][4], x[1][5]))
    # ----------------- Done --------------------- #

    # Convert rdd back to sparkDF
    sparkDF = rdd4.toDF(['Datetime', 'Open', 'High', 'Low', 'Close', 'Adj_Close', 'Volume'])

    # Write sparkDF to HDFS
    sparkDF.write.parquet(join(hdfs_save_dir, file))

print(f" --- Done --- ")

########################################
# Depricated Code
########################################

# if args.prod:
#     client = InsecureClient('http://bdm@localhost:9870', user='bdm')
# else:
#     client = Config().get_client('dev')


# todays_date_str = datetime.now().strftime("%Y-%m-%d")
# base_dir = join("stock", "ohlc")
# data_dir = join(base_dir, todays_date_str)
# hdfs_save_dir = "stock"

# for file in listdir(data_dir):
#     print(f"Saving file: {file}")
#     # Writing using the Avro format (If file name on HDFS exists it gives "Connection reset by peer" )
#     with open(join(data_dir, file) , 'r') as reader, AvroWriter(client, join(hdfs_save_dir, file) ) as writer:
#         writer.write(reader.read())
# print(f" --- Done --- ")
