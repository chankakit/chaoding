#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import os

import akshare as ak
import sqlite3 as sql
import pandas as pd

# 每日更新

# 获取所有 A 股，但这个不含上市日期
# all_a = ak.stock_info_a_code_name()
# all_a.to_sql("ALL_A", conn, if_exists="replace")

# 分别从上交所和深交所获取所有现役 A 股，并写到数据库里
# 每日收市后运行更新数据
# 数据库名称：stock_a_alive.db，内含 3 个表
def get_lateset_a_gu_list():
  sh_a = ak.stock_info_sh_name_code("主板A股")
  sh_kc = ak.stock_info_sh_name_code("科创板")
  sz_a = ak.stock_info_sz_name_code("A股列表")
  if not os.path.exists('../database/'):
    print('database dir not exist, create one')
    os.mkdir('../database')
  with sql.connect('../database/stock_a_alive.db') as conn:
    sh_a.to_sql("SHANGHAI_A", conn, if_exists="replace")
    sh_kc.to_sql("SHANGHAI_KECHUANG", conn, if_exists="replace")
    sz_a.to_sql("SHENZHEN_A", conn, if_exists="replace")

# 从每日更新到的数据库里，提取所有股票列表的「代码」、「简称」、「上市日期」，再存进分表数据库里
# 每日收市后运行更新数据
# 数据库名称：stock_a.db，
# 内含 4 个表
# sz_zhuban: 深圳主板
# sz_chaungyeban: 深圳创业板
# sh_zhuban: 上海主板
# sh_kechuangban: 上海科创板
def shenzhen_process():
  with sql.connect('../database/stock_a_alive.db') as conn:
    sz_zhuban = pd.read_sql('SELECT "A股代码", "A股简称", "A股上市日期" FROM SHENZHEN_A WHERE "板块" == "主板"', conn)
    sz_chuangyeban = pd.read_sql('SELECT "A股代码", "A股简称", "A股上市日期" FROM SHENZHEN_A WHERE "板块" == "创业板"', conn)
    
    sz_zhuban = sz_zhuban.rename(columns={'A股代码': 'company_code', 'A股简称': 'company_abbr', 'A股上市日期': 'listing_date'})
    sz_chuangyeban = sz_chuangyeban.rename(columns={'A股代码': 'company_code', 'A股简称': 'company_abbr', 'A股上市日期': 'listing_date'})
    # print(sz_zhuban)
    # print(sz_chuangyeban)
    with sql.connect('../database/stock_a.db') as stock_a_conn:
      sz_zhuban.to_sql("sz_zhuban", stock_a_conn, if_exists='replace')
      sz_chuangyeban.to_sql("sz_chuangyeban", stock_a_conn, if_exists='replace')

def shanghai_process():
  with sql.connect('../database/stock_a_alive.db') as conn:
    sh_zhuban = pd.read_sql('SELECT "COMPANY_CODE", "COMPANY_ABBR", "LISTING_DATE" FROM SHANGHAI_A', conn)
    sh_kechuangban = pd.read_sql('SELECT "COMPANY_CODE", "COMPANY_ABBR", "LISTING_DATE" FROM SHANGHAI_KECHUANG', conn)
    
    sh_zhuban = sh_zhuban.rename(columns={'COMPANY_CODE': 'company_code', 'COMPANY_ABBR': 'company_abbr', 'LISTING_DATE': 'listing_date'})
    sh_kechuangban = sh_kechuangban.rename(columns={'COMPANY_CODE': 'company_code', 'COMPANY_ABBR': 'company_abbr', 'LISTING_DATE': 'listing_date'})
    # print(sh_zhuban)
    # print(sh_kechuangban)
    with sql.connect('../database/stock_a.db') as stock_a_conn:
      sh_zhuban.to_sql("sh_zhuban", stock_a_conn, if_exists='replace')
      sh_kechuangban.to_sql("sh_kechuangban", stock_a_conn, if_exists='replace')


if __name__=='__main__':
  get_lateset_a_gu_list()
  shenzhen_process()
  shanghai_process()
