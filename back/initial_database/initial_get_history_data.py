#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from multiprocessing import Process
from datetime import date
import sqlite3 as sql
from datetime import date, datetime
import time

import pandas as pd
import numpy as np

import akshare as ak

# 获取历史数据，参数：股票代码（'600000'），起始日期（'20200101'），结束日期（'20210101'）
def get_stock_a_hist(code, start_date, end_date):
  code_str = str(code)
  start_date_str = str(start_date)
  end_date_str = str(end_date)
  stock_zh_a_hist_df = ak.stock_zh_a_hist(symbol=code_str, start_date=start_date_str, end_date=end_date_str, adjust="qfq")
  return stock_zh_a_hist_df
    
def get_bk_part_data(stock_bk_part, start_date_str, end_date_str, bk_name):
  total_length = len(stock_bk_part['company_code'])
  for index, stock_code in enumerate(stock_bk_part['company_code']):
    print(f'{index} - {round((index+1)/total_length*100, 2)}% | running at: {stock_code}.')
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


# 获取某板块历史数据
# 参数：板块股票列表（DataFrame），起始日期（'20200101'），结束日期（'20210101'），板块名称（'sh_zhuban'）
def get_bk_stock_data(stock_bk, start_date_str, end_date_str, bk_name):
  # 按 company_code 列排序，即股票代号
  stock_bk.sort_values(by=['company_code'])
  get_bk_part_data(stock_bk, start_date_str, end_date_str, bk_name)


def get_bk_stock_data_mp(stock_bk, start_date_str, end_date_str, bk_name):
  # 按 company_code 列排序，即股票代号
  stock_bk.sort_values(by=['company_code'])
  # 8 是指每个板块的进程，4 个板块即会开 32 个进程，
  # 极限进程数不清楚，可以试试
  num_process_per_bk = 8
  bk_query = np.array_split(stock_bk, num_process_per_bk)
  
  process_list = []
  
  for index, bk_part in enumerate(bk_query):
    p = Process(target=get_bk_part_data, args=(bk_part, start_date_str, end_date_str, bk_name+'_'+str(index)))
    p.start()
    process_list.append(p)

  for p in process_list:
    p.join
  
  print("IT'S DONE!")

# 获取数据入口，mp 指多进程，默认是关的
def get_a_stock_data(bk, start_date_str, end_date_str, mp=False):
  with sql.connect('../database/stock_a.db') as conn0:
    sql_str = 'SELECT * FROM ' + bk
    bk_data = pd.read_sql(sql_str, conn0)
    if mp:
      get_bk_stock_data_mp(bk_data, start_date_str, end_date_str, bk)
    else:
      get_bk_stock_data(bk_data, start_date_str, end_date_str, bk)

if __name__=='__main__':
  start_date = '20200101'  # 数据起始日期
  end_date = datetime.today().strftime('%Y%m%d')  # 数据结束日期

  # 板块名称列表，按 update_china_a.py 里面决定
  # bks = ('sh_zhuban', 'sh_kechuangban', 'sz_zhuban', 'sz_chuangyeban')
  bks = ('sh_kechuangban', 'sz_chuangyeban')

  for bk in bks:
    get_a_stock_data(bk, start_date, end_date, True)

  print('Done.')

  # process_list = []
  # for bk in bks:
  #   p = Process(target=get_a_stock_data, args=(bk, start_date, end_date,))
  #   p.start()
  #   process_list.append(p)

  # for p in process_list:
  #   p.join()

