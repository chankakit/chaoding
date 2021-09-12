#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import pandas as pd
import sqlite3 as sql

if __name__=='__main__':
  db_file = '../database/stock_a_history/sh_zhuban.db'
  with sql.connect(db_file) as conn:
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    stocks = cursor.fetchall()
    for stock in stocks:
      print(stock[0])
      sql_str = 'SELECT * FROM ' + stock[0]
      stock_hist = pd.read_sql(sql_str, conn)
      # stock_hist['日期'] =pd.to_datetime(stock_hist['日期'])
      # stock_hist.sort_values(by=['日期'], inplace=True, ascending=False)
      # print(stock_hist[[0]])