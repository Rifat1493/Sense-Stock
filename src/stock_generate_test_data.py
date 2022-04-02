import pandas as pd
from datetime import datetime, timedelta
import yfinance as yf
from copy import deepcopy
from tqdm import tqdm

# # Create test data base file, Uncomment the following to run again and create
# # the temp data again
# dft = yf.download(tickers="ADBE", period="1d", interval="1m")
# dft.to_csv("stock/ohlc/temp.csv")

df = pd.read_csv("stock/ohlc/temp.csv")
# convert data type of col: Datetime From str -> datetime
dt = df.Datetime.apply(lambda x: datetime.fromisoformat(x))
df.Datetime = dt

# Add 1 day to current df's datetime column
next_day_datetime = df.Datetime.apply(lambda x: x + timedelta(days=1) )

NUM_ITRS = 2
df_final = pd.DataFrame()
df_final = deepcopy(df)
list_of_dfs = []
for i in tqdm( range(NUM_ITRS), total=NUM_ITRS ):

    next_day_datetime = df.Datetime.apply(lambda x: x + timedelta(days=1))
    df.Datetime = next_day_datetime
    df_final = pd.concat( ( df_final, df) )

df_final.to_csv("stock/ohlc/stock_benchmark.csv", index=False)
