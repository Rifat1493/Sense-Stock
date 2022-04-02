#!/usr/bin/env bash
/home/bdm/anaconda3/bin/python /home/bdm/sense-stock/src/stock_raw_to_hdfs.py --prod |& tee -a /home/bdm/sense-stock/logs/stock_raw_to_hdfs.log
