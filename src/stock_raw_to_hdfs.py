from hdfs import InsecureClient
from hdfs import Config
from hdfs.ext.avro import AvroWriter
from time import time

# way 1
# client = InsecureClient('http://bdm@10.4.41.75:9870', user='bdm')

# way 2
client = Config().get_client('dev')


print('---')
# Writing using the Avro format (If file name on HDFS exists it gives "Connection reset by peer" )
with open("stock/ohlc/stock_benchmark.csv", 'r') as reader, AvroWriter(client, 'test1.avro') as writer:
    writer.write(reader.read())
print('---')
