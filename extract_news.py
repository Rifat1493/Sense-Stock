from hdfs import InsecureClient
from datetime import datetime
import requests
from json import dumps
import os
dirs = "/home/bdm/proj/"
# dirs = ""

with open(dirs+'input/key.txt') as f:
    key = f.readlines()[0]


url = "https://newsapi.org/v2/top-headlines?country=us&category=business&apiKey="+key
payload = {}
headers = {}
response = requests.request("GET", url, headers=headers, data=payload)
data = response.json()
date = datetime.now().strftime("%Y_%m_%d-%H:%M:%S")
# get connected with hdfs server
client = InsecureClient("http://10.4.41.50:9870", user='bdm')

ddate = date.split("-")
# fnames = client.list('/user/bdm')
# write file to hdfs server
client.makedirs('stock/news/'+ddate[0])
tmp_fold = 'stock/news/'+ddate[0]
tmp_file = ddate[1].split(':')[0]
file_name = tmp_fold + '/file_' + tmp_file + '.json'
client.write(file_name, dumps(data))


# client.delete('stock/news/2022_03_29', recursive=True)



