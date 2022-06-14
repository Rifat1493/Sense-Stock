#!/usr/bin/env bash
/home/bdm/anaconda3/envs/py37/bin/python /home/bdm/sense-stock/src/stock_1h_agg_to_1d.py --prod |& tee -a /home/bdm/sense-stock/logs/stock_1m_agg_to_1h.log
