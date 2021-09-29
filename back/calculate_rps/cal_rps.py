#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import os
import pandas as pd
import numpy as np
import sqlite3 as sql
from datetime import datetime

def get_new_and_st(table, db_conn):
  fields = '*'
  # 筛选出不是一年内的新股且不为 ST
  # conditions = 'listing_date < date("now","-1 years") AND instr(company_abbr, "ST") = 0'
  # 筛选出上市不足一年或 ST
  conditions = 'listing_date > date("now","-1 years") OR instr(company_abbr, "ST") > 0'
  sql_string = (f'SELECT {fields} '
                f'FROM {table} '
                f'WHERE {conditions};')

  stock_without_new_and_st = pd.read_sql(sql_string, db_conn)
  return stock_without_new_and_st

def get_all_stock(table, db_conn):
  fields = 'company_code, company_abbr'
  sql_string = (f'SELECT {fields} '
                f'FROM {table} ')
  all_stock_with_abbr = pd.read_sql(sql_string, db_conn)
  return all_stock_with_abbr


def cal_price_change(rps_days):
  # 构造列
  # ['company_code', 'bk', 'change_5_pct', 'change_10_pct', 'change_20_pct', 'change_60_pct', 'change_120_pct', 'change_250_pct']
  db_columns = ['company_code', 'bk']
  for rps in rps_days:
    db_columns.append(f'change_{rps}_pct')

  db_dir_path = '../database/stock_a_history/'
  db_files = ('sh_zhuban', 'sh_kechuangban', 'sz_zhuban', 'sz_chuangyeban')

  
  stocks_change_data = []

  for db_file in db_files:
    with sql.connect(db_dir_path + db_file + '.db') as conn:
      cursor = conn.cursor()
      cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
      stocks = cursor.fetchall()

      for stock in stocks:
        # 初始化每个股票数据数组
        # 根据上面的构造列顺序，第 25 行
        # 因为历史数据每个表的表名是 交易所所在地两位拼音首字母+数字，所以 company_code 要去到拼音
        column_data = [stock[0][2:8], db_file]

        filed = '收盘'
        # 获取最新日前的收盘价
        sql_str_latest = f'SELECT 1, {filed} FROM {stock[0]} ORDER BY date("日期") DESC LIMIT 1'
        stock_latest_price_df = pd.read_sql(sql_str_latest, conn)

        for rps_day in rps_days:
          day = rps_day
          # 读取 x 日前的收盘价
          sql_str_before = f'SELECT {day}, {filed} FROM {stock[0]} ORDER BY date("日期") DESC LIMIT 1 OFFSET {day}'
          stock_x_day_price_df = pd.read_sql(sql_str_before, conn)
          

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

  return df
  
if __name__=='__main__':
  # 计算所有股票的不同日子涨跌幅百分比
  RPS_DAYS = (5, 10, 20, 60, 120, 250)
  price_change_df = cal_price_change(RPS_DAYS)
  
  tables = ('sh_zhuban', 'sh_kechuangban', 'sz_zhuban', 'sz_chuangyeban')

  
  with sql.connect('../database/stock_a.db') as conn:
    # 获取所有股票代码和简称
    get_all_stock_vfunc = np.vectorize(get_all_stock, cache=False)
    stock_with_abbr = get_all_stock_vfunc(tables, conn)
    # 筛选出 ST 和 上市不满一年的股票列表
    get_new_and_st_vfunc = np.vectorize(get_new_and_st, cache=False)
    stocks_ndarray = get_new_and_st_vfunc(tables, conn)

  all_stock_with_abbr_df = pd.concat(stock_with_abbr, ignore_index=True)

  # 从所有股票的涨跌值表中去除 ST 和上市不满一年的
  for bk in stocks_ndarray:
    remove_list = bk['company_code'].values.tolist()
    price_change_df = price_change_df[~price_change_df['company_code'].isin(remove_list)]
  
  price_change_df = pd.merge(price_change_df, all_stock_with_abbr_df, on=['company_code'])
  if not os.path.exists('../database/rps/price_change_no_new_st.db'):
    print('rps dir not exist, create one')
    os.mkdir('../database/rps/')
    
  with sql.connect('../database/rps/price_change_no_new_st.db') as pc_test_conn:
    price_change_df.to_sql('test', pc_test_conn, if_exists='replace')

  # 去除后的股票数量
  stock_count = price_change_df.shape[0]
  # 初始化 rps 数据 df
  all_rps_df = pd.DataFrame()

  for index_rps, rps in enumerate(RPS_DAYS):
    # 因为记录的时候是字符串，要转换为浮点数
    price_change_df[f'change_{rps}_pct'] = price_change_df[f'change_{rps}_pct'].astype(float)
    # 按 x 日的涨跌幅百分比排序倒序，重置索引数，赋值到新的 Dataframe
    new_df = price_change_df.sort_values(by=[f'change_{rps}_pct'], ascending=False).reset_index(drop=True)
    
    company_code = []
    rps_x = []
    # 行遍历新的 Dataframe，计算排名标准分，就是 RPS 指标
    for index, row in new_df.iterrows():
      # print(f'{row["company_code"]} | RPS_{rps}: {round((1 - (index + 1) / stock_count) * 100, 2)}%')
      company_code.append(row['company_code'])
      rps_x.append(round((1 - (index + 1) / stock_count) * 100, 2))
      # with sql.connect('../database/rps/test.db') as rps_test_conn:
      #   new_df.to_sql('test', rps_test_conn, if_exists='replace')
    
    rps_x_df = pd.DataFrame({'company_code': company_code, f'rps_{rps}': rps_x})
    if index_rps == 0:
      all_rps_df = pd.merge(all_stock_with_abbr_df, rps_x_df, on=['company_code'])
      # all_rps_df = rps_x_df
    else:
      all_rps_df = pd.merge(all_rps_df, rps_x_df, on=['company_code'])
  
  # print(all_rps_df)
  today = datetime.today().strftime('%Y%m%d')
  all_rps_df.to_excel(f'../database/rps/rps_{today}.xlsx')
  # print(new_df[['company_code', 'change_5_pct']])
    # print(bk)
    # print(type(bk['company_code']))