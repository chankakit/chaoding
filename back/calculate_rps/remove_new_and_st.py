#!/usr/bin/env python
# -*- coding: UTF-8 -*-

# import akshare as ak
import sqlite3 as sql
import pandas as pd
import numpy as np

# 从数据库中读取上市时间大于 1 年的股票且不为 ST
def get_all_stock_without_new_and_st(table, db_conn):
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


with sql.connect('database/stock_a.db') as conn:
  tables = ('sh_zhuban', 'sh_kechuangban', 'sz_zhuban', 'sz_chuangyeban')

  vectfunc = np.vectorize(get_all_stock_without_new_and_st, cache=False)
  stocks_ndarray = vectfunc(tables, conn)

  for bk in stocks_ndarray:
    # print(bk[['company_code','company_abbr']])
    # print(bk)
    print(type(bk['company_code']))

