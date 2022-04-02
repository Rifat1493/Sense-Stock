import argparse
from hdfs import InsecureClient
from hdfs import Config
from hdfs.ext.avro import AvroWriter
from time import time
from os.path import isfile
from os import getcwd

"""
Usage examples:
Run from outside the src directory:
    python src/stock_test_hdfs_formats.py <path_to_file_being_upload> <name_on_hdfs> --avro --serve

Example commands - run from sense-stock directory:
Save plain text file
    python src/stock_test_hdfs_formats.py ~/Downloads/stock_benchmark.csv stock.txt --server

Save Avro format file
    python src/stock_test_hdfs_formats.py ~/Downloads/stock_benchmark.csv stock.txt --server

"""

# Instantiate the parser
parser = argparse.ArgumentParser(description='This is to facilitate testing different file formats on'
                                             'HDFS. You specify the file path and format')

parser.add_argument('path', type=str,
                    help='Path on local machine to the file to upload')

parser.add_argument('name_on_hdfs', type=str,
                    help='Name of the file on HDFS')

parser.add_argument('--avro', action='store_true',
                    help='Flag: If specified format is avro else plain text')

parser.add_argument('--server', action='store_true',
                    help='Flag: If specified code is on upc-vm server else on local machine')

args = parser.parse_args()

print(f"Current dir: {getcwd()}")

# Check if specified path is of a file
if not isfile(args.path):
    raise ValueError(f"Path: {args.path} is not of a file")

# way 1
if args.server:
    client = InsecureClient('http://bdm@localhost:9870', user='bdm')
else:
    client = InsecureClient('http://bdm@10.4.41.75:9870', user='bdm')


if args.avro:
    print(f"Running test: Put Stock CSV as AVRO in HDFS")
    start_time = time()
    # If the file already exists it will give "peer connection error"
    with open(args.path, 'r') as reader, AvroWriter(client, args.name_on_hdfs) as writer:
        writer.write(reader.read())
    end_time = time()
else:
    print(f"Running test: Put Stock CSV as plain-text in HDFS")
    start_time = time()
    # If the file already exists it will give "peer connection error"
    with open(args.path, 'rb') as reader, client.write(args.name_on_hdfs) as writer:
        writer.write(reader.read())
    end_time = time()
print(f'Time it took to save {args.path}: { (end_time - start_time) } seconds')
print("---")
