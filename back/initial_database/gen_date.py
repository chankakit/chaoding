#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from datetime import date, datetime

def gen_yyyymmdd_list(start_year):
  this_year = datetime.today().year
  this_month = datetime.today().month

  yyyymmdd_list = []

  # 往年
  for year in range(start_year, this_year):
    for month in range(12):
      yyyymmdd_list.append(date(year, month + 1, 1))
      
  # 今年
  for month in range(this_month + 1):
    yyyymmdd_list.append(date(this_year, month + 1, 1))

  return yyyymmdd_list

if __name__=='__main__':
  start_year = 2020
  print(gen_yyyymmdd_list(start_year))
  print(len((gen_yyyymmdd_list(start_year))))