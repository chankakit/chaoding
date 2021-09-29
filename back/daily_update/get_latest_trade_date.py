#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from random import randint
from datetime import datetime
import sqlite3 as sql
import pandas as pd

stock_a_history_merged_path = '../database/stock_a_history/'

def get_latest_trade_date():
  bk = 'sh_zhuban'
  with sql.connect(stock_a_history_merged_path + bk + '.db') as merged_conn:
    cursor = merged_conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    stocks = cursor.fetchall()
    latest_trade_date = datetime.strptime("1990-01-01", "%Y-%m-%d")
    
    # 随机抽取20只股票，查看最后交易日期，对比取最新的，希望不会都抽中停牌的。。。
    for i in range(20):
      stk_code =stocks[randint(0, len(stocks)-1)][0]
      sql_str = 'SELECT * FROM ' + stk_code + ' ORDER BY date("日期") DESC LIMIT 1'
      temp_date_str = pd.read_sql(sql_str, merged_conn)['日期'][0]
      temp_date = datetime.strptime(temp_date_str, "%Y-%m-%d")
      if temp_date > latest_trade_date:
        latest_trade_date = temp_date
    
    return latest_trade_date.strftime('%Y-%m-%d')

if __name__=='__main__':
  bks = ('sh_zhuban', 'sh_kechuangban', 'sz_zhuban', 'sz_chuangyeban')
  print(get_latest_trade_date())