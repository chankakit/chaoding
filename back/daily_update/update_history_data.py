#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import os
from multiprocessing import Process, Pool
import sqlite3 as sql
from datetime import datetime
import time
import pandas as pd
import numpy as np
import akshare as ak


__a_stk_ls_db = '../database/stock_a.db'
__a_stk_hist_dir = '../database/stock_a_history/'
__a_stk_hist_blocks_dir = __a_stk_hist_dir + 'blocks/'


# 目录检测创建
def gen_dir(dir_path):
  if not os.path.exists(dir_path):
    os.mkdir(dir_path)


# 获取历史数据，参数：股票代码（'600000'），起始日期（'20200101'），结束日期（'20210101'）
def get_stock_a_hist_and_write(stock_code, start_date_str, end_date_str, bk_name, db_path):
  stock_zh_a_hist_df = ak.stock_zh_a_hist(symbol=stock_code, start_date=start_date_str, end_date=end_date_str, adjust="qfq")
  database_file = db_path + bk_name + '_history.db'
  with sql.connect(database_file) as conn:
    table_name = bk_name[:2] + stock_code
    stock_zh_a_hist_df.to_sql(table_name, conn, if_exists="replace")
    

def get_bk_part_data(stock_bk_part, start_date_str, end_date_str, bk_name, db_path):
  total_length = len(stock_bk_part['company_code'])
  for index, stock_code in enumerate(stock_bk_part['company_code']):
    # print(f'{index} - {round((index+1)/total_length*100, 2)}% | running at: {stock_code}.')
    try:
      get_stock_a_hist_and_write(stock_code, start_date_str, end_date_str, bk_name, db_path)
      if index % 30 == 29:
        print(f'{index} - {round((index+1)/total_length*100, 1)}% | running at: {stock_code}.')
        # 每隔 30 只股票休息 5 秒，防封
        print('----------- SLEEP 5s -----------')
        time.sleep(5)
    except:
      # 异常则先休息 10 秒，然后再试
      print(f"EXCEPTION ON: {stock_code}")
      print('----------- SLEEP 10s -----------')
      time.sleep(10)
      get_stock_a_hist_and_write(stock_code, start_date_str, end_date_str, bk_name, db_path)


# 获取某板块历史数据
# 参数：板块股票列表（DataFrame），起始日期（'20200101'），结束日期（'20210101'），板块名称（'sh_zhuban'）
def get_bk_stock_data(stock_bk, start_date_str, end_date_str, bk_name, db_path):
  # 按 company_code 列排序，即股票代号
  stock_bk.sort_values(by=['company_code'])
  get_bk_part_data(stock_bk, start_date_str, end_date_str, bk_name, db_path)


def get_bk_stock_data_mp(stock_bk, start_date_str, end_date_str, bk_name, db_path):
  # 按 company_code 列排序，即股票代号
  stock_bk.sort_values(by=['company_code'])
  # 8 是指每个板块的进程，4 个板块即会开 32 个进程，
  # 极限进程数不清楚，可以试试
  num_process_per_bk = 8
  bk_query = np.array_split(stock_bk, num_process_per_bk)
  
  process_list = []
  
  for index, bk_part in enumerate(bk_query):
    p = Process(target=get_bk_part_data, args=(bk_part, start_date_str, end_date_str, bk_name+'_'+str(index), db_path))
    process_list.append(p)
    p.start()
  
  return process_list
  

# 获取数据入口，mp 指多进程，默认是关的
def get_a_stock_data(bk, start_date_str, end_date_str, mp=False):
  gen_dir(__a_stk_hist_dir)
  with sql.connect(__a_stk_ls_db) as a_stk_db_conn:
    sql_str = 'SELECT * FROM ' + bk
    bk_data = pd.read_sql(sql_str, a_stk_db_conn)
    if mp:
      gen_dir(__a_stk_hist_blocks_dir)
      process_list = get_bk_stock_data_mp(bk_data, start_date_str, end_date_str, bk, __a_stk_hist_blocks_dir)
      for p in process_list:
        p.join()
      print('done')
    else:
      get_bk_stock_data(bk_data, start_date_str, end_date_str, bk, __a_stk_hist_dir)


if __name__=='__main__':
  # 日期
  start_date = '20200101'  # 数据起始日期
  end_date = datetime.today().strftime('%Y%m%d')  # 数据结束日期

  # 板块名称列表，按 update_a_stock.py 里面决定
  bks = ('sh_zhuban', 'sh_kechuangban', 'sz_zhuban', 'sz_chuangyeban')
  for bk in bks:
    get_a_stock_data(bk, start_date, end_date, True)
