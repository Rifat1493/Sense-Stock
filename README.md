# sense-stock
This is a project for BDMA 2nd Semester at UPC, Barcelona.

## Notes

- Saving data from daily CSV to single file on HDFS. File on HDFS has company name.

### Setup Instructions
Create a log folder
```bash
mkdir logs
```
Use the script to [connect to VPN](docs/connect_vpn.sh)

Create the required directory on Hadoop HDFS
```bash
hdfs dfs -ls /user/bdm/stock
```

#### Input companies
In src there is a text file `list_of_companies.txt` which contains a list of companies for which
the program runs. The structure of the file is, one company symbol per line:
```text
ATVI
ADBE
GOOGL
```
If you want to add more companies, just add its symbol in the file on a new line.

### Cronjobs
For local setup: Cronjob command to run the `fetch_ohlc_data.py` at 5th minute of every hour. It basically executes the
file `run.sh` which has the full command with arguments.

    5 */1 * * * /home/teemo/MEGA/bdma-semesters/2-semester/sense-stock/run.sh

For upc-vm setup: Cronjob command to run the `fetch_ohlc_data.py` at 5th minute of every hour. It basically executes the
file `run_server.sh` which has the full command with arguments.

    5 */1 * * * /home/bdm/sense-stock/run_server.sh

#### Once per night, copy files from local directory to HDFS
For upc-vm setup: Cronjob command to run the `src.stock_raw_to_hdfs.py` at 23:00.

    0 23 * * * /home/bdm/sense-stock/run_persistent_landing.sh

For upc-vm setup: Cronjob command to run the `src.stock_1m_agg_to_1h.py` at 23:00.

    10 23 * * * /home/bdm/sense-stock/stock_1m_agg_to_1h.bash

For upc-vm setup: Cronjob command to run the `src.stock_1h_agg_to_1d.py` at 23:00.

    20 23 * * * /home/bdm/sense-stock/stock_1h_agg_to_1d.bash

Cronjob command to run the `extract_news.py` at 3 hours interval to extract news from the news api.

    0 */3 * * * /usr/bin/python3 /home/bdm/proj/extract_news.py

#### Note on Writing bash scripts that run the python script
You need to activate conda environment in the bash script:
https://stackoverflow.com/questions/55507519/python-activate-conda-env-through-shell-script


### Instructions to Benchmark different file formats on HDFS
Usage is explained in the file `stock_test_hdfs_formats.py`. Examples are also given.

Example of output:
![img](docs/benchmark_results.png)

### HDFS
#### HDFS Config
Following is the default file and location of the config file. 
` ~/.hdfscli.cfg`

### List of companies
List of companies for which we are working

| Symbol | Company Name           |
|-------|------------------------|
| ATVI  | Activision Blizzard    |
| ADBE  | Adobe                  |
| GOOGL | Alphabet               |
| AMZN	 | Amazon                 |
| AMD 	 | AMD                    |
| AAPL  | Apple                  |
| CMG   | Chipotle Mexican Grill |
| CSCO  | Cisco                  |
| DIS 	 | Disney                 |
| DPZ 	 | Domino's               |
| INTC	 | Intel                  |
| FB    | Meta                   |
| MCHP  | Microchip              |
| NFLX  | Netflix                |
| NKE   | Nike                   |
| TSLA  | Tesla                  |


### Access UPC virtual machines
https://virtech.fib.upc.edu/
user: masterBD11
pass: learnSQL - team creator link

## Resources
If you want to put files on server using command line from python
https://stackoverflow.com/questions/26606128/how-to-save-a-file-in-hadoop-with-python
