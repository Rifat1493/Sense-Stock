# from hdfs.ext.avro import AvroWriter
from datetime import datetime, timedelta
from os import listdir
from os.path import join
from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType, StructType, StructField, StringType, IntegerType, DoubleType
# from avro.datafile import DataFileReader, DataFileWriter
# from avro.io import DatumReader, DatumWriter
import pandas as pd
import mplfinance as fplt

spark = SparkSession.builder \
    .appName("ABC app name") \
    .master("local[1]") \
    .getOrCreate()

print(" ------------- Start -------------")
print(f"File Run: {datetime.now()}")

# company = "ADBE"

with open("src/list_of_companies.txt", "r") as fh:
    list_of_companies = fh.readlines()
list_companies = list( map(lambda x: x.strip(), list_of_companies) )

for company in list_companies:

    todays_date_str = datetime.now().strftime("%Y-%m-%d")
    todays_date = datetime.now()

    cutoff_date = datetime(todays_date.year, todays_date.month, todays_date.day)  # Get start of the day

    base_dir = join("stock", "ohlc")
    data_dir = join(base_dir, todays_date_str)
    hdfs_save_dir = "stock"

    # load file from HDFS
    sparkDF = spark.read.parquet(f"stock/{company}")
    #Create RDD from sparkDF
    rdd = sparkDF.rdd
    #  ------------- Do processing --------------- #

    # # # Filter data that is too old for this category of aggregation
    rdd1 = rdd.filter(lambda x: cutoff_date < x[0])

    # # # take Average of all the statistics over 1 day
    #                                                                                  #open, high, low, close, Adj_Close, Volume, 1-for-help-in-avg-later
    rdd2 = rdd1.map( lambda x: ( datetime(x[0].year, x[0].month, x[0].day ) , (x[1], x[2], x[3], x[4], x[5], x[6], 1) ) )
    # add all
    rdd3 = rdd2.reduceByKey( lambda x, y: ( x[0] + y[0],
                                            x[1] + y[1],
                                            x[2] + y[2],
                                            x[3] + y[3],
                                            x[4] + y[4],
                                            x[5] + y[5],
                                            x[6] + y[6] ) )

    rdd4 = rdd3.map(lambda x: ( x[0], x[1][0] / x[1][6],  # Open / count-vals
                                    x[1][1] / x[1][6],  # High / Count-vals
                                    x[1][2] / x[1][6],  # Low / count-vals
                                    x[1][3] / x[1][6],  # Close / count-vals
                                    x[1][4] / x[1][6],  # Adj_Close / count-vals
                                    x[1][5] / x[1][6], ) )  # Volume / count-vals




    # ----------------- Done --------------------- #
    print("initial partition count:"+str(rdd4.getNumPartitions()))
    # Convert rdd back to sparkDF
    sparkDF = rdd4.toDF(['Datetime', 'Open', 'High', 'Low', 'Close', 'Adj_Close', 'Volume'])

    # Write sparkDF to HDFS
    sparkDF.write.mode('append').parquet(f"stock-1h/{company}.prq")
