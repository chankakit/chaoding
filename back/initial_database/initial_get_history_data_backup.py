#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from multiprocessing import Process
from datetime import date
import sqlite3 as sql
from datetime import date, datetime
import time

import pandas as pd

import akshare as ak

def get_stock_a_hist(code, start_date, end_date):
  code_str = str(code)
  start_date_str = str(start_date)
  end_date_str = str(end_date)
  stock_zh_a_hist_df = ak.stock_zh_a_hist(symbol=code_str, start_date=start_date_str, end_date=end_date_str, adjust="qfq")
  return stock_zh_a_hist_df
    

def get_bk_stock_data(stock_bk, start_date_str, end_date_str, bk_name):
  # 按 company_code 列排序，即股票代号
  stock_bk.sort_values(by=['company_code'])
  total_length = len(stock_bk['company_code'])
  for index, stock_code in enumerate(stock_bk['company_code']):
    print(f'{index} - {round(index/total_length*100, 2)}% | running at: {stock_code}.')
    try:
      stock_zh_a_hist_df = ak.stock_zh_a_hist(symbol=stock_code, start_date=start_date_str, end_date=end_date_str, adjust="qfq")
      database_file = '../database/stock_a_history/'+ bk_name + '_history.db'
      with sql.connect(database_file) as conn:
        table_name = bk_name[:2] + stock_code
        stock_zh_a_hist_df.to_sql(table_name, conn, if_exists="replace")
      if index % 30 == 29:
        # 每隔 30 只股票休息 5 秒，防封
        print('----------- SLEEP 5s -----------')
        time.sleep(5)
    except:
      # 异常则先休息 10 秒，然后再试
      print(f"EXCEPTION ON: {index}")
      print('----------- SLEEP 10s -----------')
      time.sleep(10)
      stock_zh_a_hist_df = ak.stock_zh_a_hist(symbol=stock_code, start_date=start_date_str, end_date=end_date_str, adjust="qfq")
      database_file = '../database/stock_a_history/'+ bk_name + '_history.db'
      with sql.connect(database_file) as conn:
        stock_zh_a_hist_df.to_sql(stock_code, conn, if_exists="replace")

def get_a_stock_data(start_date_str, end_date_str):
  tables = ('sz_zhuban', 'sz_chuangyeban')
  with sql.connect('../database/stock_a.db') as conn0:
    for bk in tables:
      sql_str = 'SELECT * FROM ' + bk
      bk_data = pd.read_sql(sql_str, conn0)
      get_bk_stock_data(bk_data, start_date_str, end_date_str, bk)
    

if __name__=='__main__':
  start_date = '20200101'
  end_date = datetime.today().strftime('%Y%m%d')
  get_a_stock_data(start_date, end_date)
