# import nasdaqdatalink
import argparse
import pandas as pd
import yfinance as yf
from datetime import datetime
from os import mkdir
from os.path import exists, join

OUTPUT_DIR = ""


# command get data for "ADBE"
data = yf.download(tickers="ADBE", period="1h", interval="1m")

# Test Saving data properly
data.to_csv("temp.csv")

ndata = pd.read_csv("temp.csv", index_col="Datetime")

# ---------  code library
########################################
# for testing
########################################

# Test case where data overlaps in the dfs
# start = datetime(2022, 3, 29, 14)
# end = datetime(2022, 3, 29, 15)
#
# df1 = yf.download(tickers="ADBE", period='2h', interval="1m")
# df2 = yf.download(tickers="ADBE", period='1h', interval="1m")


# date_str = datetime.now().strftime("%Y-%m-%d")

# print(data)

# data.to_csv(f"/home/teemo/MEGA/bdma-semesters/2-semester/sense-stock/{date_str}_ATVI_stocks.csv")

