#!/usr/bin/env bash
/home/bdm/anaconda3/bin/python /home/bdm/sense-stock/src/stock_fetch_ohlc_data.py --prod |& tee -a /home/bdm/sense-stock/logs/stock_fetch_ohlc.log
