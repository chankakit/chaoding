#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import os
from multiprocessing import Process
import pandas as pd
import sqlite3 as sql

def get_data_and_write_database(stock, source_db_conn, merged_db_conn):
  sql_str = 'SELECT * FROM ' + stock
  # print(sql_str)
  pd.read_sql(sql_str, source_db_conn).to_sql(stock, merged_db_conn, if_exists="replace")

def merge_db(dirpath, files, bk):
  for file in files:
    if bk in file:
      print(file)
      with sql.connect(dirpath + file) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        stocks = cursor.fetchall()
        with sql.connect(dirpath + bk + '.db') as merged_conn:
          for stock in stocks:
            get_data_and_write_database(stock[0], conn, merged_conn)

if __name__=='__main__':
  print('merge database.')
  
  # 板块名称列表，按 update_china_a.py 里面决定
  bks = ('sh_zhuban', 'sh_kechuangban', 'sz_zhuban', 'sz_chuangyeban')

  stock_a_history_path = '../database/stock_a_history/'
  for (dirpath, dirnames, filenames) in os.walk(stock_a_history_path):
    process_list = []
    for bk in bks:
      p = Process(target=merge_db, args=(dirpath, filenames, bk))
      p.start()
      process_list.append(p)
    
    for p in process_list:
      p.join
