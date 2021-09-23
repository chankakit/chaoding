#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import update_a_stock
import update_history_data
import merge_database
from datetime import datetime


if __name__=='__main__':
  print('update a stock list')
  update_a_stock.get_a_stk_lsit()
  update_a_stock.shenzhen_process()
  update_a_stock.shanghai_process()

  print('update a stock history data')
  # 日期
  start_date = '20200101'  # 数据起始日期
  end_date = datetime.today().strftime('%Y%m%d')  # 数据结束日期

  # 板块名称列表，按 update_a_stock.py 里面决定
  bks = ('sh_zhuban', 'sh_kechuangban', 'sz_zhuban', 'sz_chuangyeban')
  for bk in bks:
    update_history_data.get_a_stock_data(bk, start_date, end_date, True)

  print('merge database blocks')
  merge_database.merge_db_entrance()
