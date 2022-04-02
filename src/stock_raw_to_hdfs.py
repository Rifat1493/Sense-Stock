from hdfs import InsecureClient
from hdfs import Config
import argparse
from hdfs.ext.avro import AvroWriter
from datetime import datetime
from os import listdir
from os.path import join

# Instantiate the parser
parser = argparse.ArgumentParser(description="Stores files in current date's directory in HDFS")

# Required positional argument
parser.add_argument('--prod', action='store_true',
                    help='Flag that tells whether script is running on dev machine or prod upc-vm server')
args = parser.parse_args()

# way 1
if args.prod:
    client = InsecureClient('http://bdm@localhost:9870', user='bdm')
else:
    client = Config().get_client('dev')


todays_date_str = datetime.now().strftime("%Y-%m-%d")
base_dir = join("stock", "ohlc")
data_dir = join(base_dir, todays_date_str)
hdfs_save_dir = "stock"

for file in listdir(data_dir):
    print(f"Saving file: {file}")
    # Writing using the Avro format (If file name on HDFS exists it gives "Connection reset by peer" )
    with open(join(data_dir, file) , 'r') as reader, AvroWriter(client, join(hdfs_save_dir, file) ) as writer:
        writer.write(reader.read())

