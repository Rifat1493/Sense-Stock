# sense-stock
This is a project for BDMA 2nd Semester at UPC, Barcelona.

### Setup Instructions
Create a log folder
```bash
mkdir logs
```
Use the script to [connect to VPN](docs/connect_vpn.sh)

To run the `fetch_ohlc_data` script on local:
```bash
python fetch_ohlc_data
```

To run the `fetch_ohlc_data` script on UPC-vm server:
```bash
python fetch_ohlc_data --prod
```

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

## Resources
If you want to put files on server using command line from python
https://stackoverflow.com/questions/26606128/how-to-save-a-file-in-hadoop-with-python
