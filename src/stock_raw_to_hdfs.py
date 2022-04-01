from hdfs import InsecureClient
from hdfs import Config

# way 1
client = InsecureClient('http://bdm@10.4.41.75:9870', user='bdm')

# way 2
client = Config().get_client('dev')


with open('README.md', 'r') as reader, client.write('samples') as writer:
    for line in reader:
        writer.write(line)
