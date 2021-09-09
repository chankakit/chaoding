#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import akshare as ak

import json

from typing import Optional
from fastapi import FastAPI

app = FastAPI()


@app.get("/")
def read_root():
  return {"Hello": "World"}


@app.get("/stock/{exchange}")
def read_item(exchange: str, borad: Optional[str] = None):
  if(exchange=="sz"):
    stock_list = ak.stock_info_sz_name_code("A股列表")[["A股代码", "A股简称", "A股上市日期"]]
  elif(exchange=="sh"):
    stock_list = ak.stock_info_sh_name_code("主板A股")[[ "COMPANY_CODE", "COMPANY_ABBR", "LISTING_DATE"]]
  else:
    stock_list = ak.stock_info_a_code_name()
  
  stock_list_json = stock_list.to_json(orient="records")
  stock_list_json = json.loads(stock_list_json)

  return {"Exchange": stock_list_json, "borad": borad}
