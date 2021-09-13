#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import os
import pandas as pd
import numpy as np
import sqlite3 as sql

if __name__=='__main__':
  RPS_DAYS = (5, 10, 20, 60, 120, 250)
  db_columns = ['company_code', 'bk']
  for rps in RPS_DAYS:
    db_columns.append(f'change_{rps}_pct')
  # print(db_columns)
  # ['company_code', 'bk', 'change_5_pct', 'change_10_pct', 'change_20_pct', 'change_60_pct', 'change_120_pct', 'change_250_pct']

  db_dir_path = '../database/stock_a_history/'
  db_files = ('sh_zhuban', 'sh_kechuangban', 'sz_zhuban', 'sz_chuangyeban')
  # db_file = 'sh_zhuban'
  change_db_dir_path = '../database/rps/'
  change_db_file = 'all_change'

  if not os.path.exists(change_db_dir_path):
    print('rps dir not exist, create one')
    os.mkdir(change_db_dir_path)
  
  stocks_change_data = []

  for db_file in db_files:
    with sql.connect(db_dir_path + db_file + '.db') as conn:
      cursor = conn.cursor()
      cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
      stocks = cursor.fetchall()

      with sql.connect(change_db_dir_path + change_db_file + '.db') as change_db_conn:
        
        for stock in stocks:
          # print(stock[0])
          
          column_data = [stock[0], db_file]

          filed = '收盘'
          # 选择最新日前的收盘价
          sql_str_latest = f'SELECT 1, {filed} FROM {stock[0]} ORDER BY date("日期") DESC LIMIT 1'
          stock_latest_price_df = pd.read_sql(sql_str_latest, conn)

          for rps_day in RPS_DAYS:
            day = rps_day
            # 选择 x 日前的收盘价
            sql_str_before = f'SELECT {day}, {filed} FROM {stock[0]} ORDER BY date("日期") DESC LIMIT 1 OFFSET {day}'
            stock_x_day_price_df = pd.read_sql(sql_str_before, conn)
            
            # print(stock_x_day_price)
            if not stock_x_day_price_df.empty:
              stock_latest_price = stock_latest_price_df.iloc[0][filed]
              stock_x_day_price = stock_x_day_price_df.iloc[0][filed]

              x_day_change_price = stock_latest_price - stock_x_day_price
              x_day_change_percent = round(x_day_change_price / stock_x_day_price * 100, 2)

              column_data.append(x_day_change_percent)
              # print(f'LATEST: {stock_latest_price} | {day} days ago: {stock_x_day_price} / {x_day_change_percent}%')
            else:
              x_day_change_percent = 0
              column_data.append(x_day_change_percent)
          
          stocks_change_data.append(column_data)
        
        df = pd.DataFrame(np.array(stocks_change_data), columns=db_columns)
        df.to_sql('change_pct', change_db_conn, if_exists="replace")
