### Start the HDFS Server
```bash
/home/bdm/BDM_Software/hadoop/sbin/start-dfs.sh
```

#### Create directory in HDFS
    hdfs dfs -ls /user/bdm

#### List files in HDFS
```bash
hdfs dfs -ls
```

Check contents of a file 
```bash
hdfs dfs -head stock_benchmark.csv
```

Check infomation about file
```bash
hdfs fsck /user/bdm/samples.avro -files -blocks -locations
```

#### Delete a file
```bash
hdfs dfs -rm file_name.txt
```

Format HDFS
```bash
hdfs namenode -format
```