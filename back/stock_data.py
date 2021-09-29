#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'test module'
__author__ = 'Nonlone Lei'

import sys
import os
import time
import akshare as ak
import sqlite3 as sql
import pandas as pd


__db_path = "database";
__db_name = "stock_a_alive.db";
__db_table_name = "stock_a";

# 写入数据库
def replace_db(df,table_name):
    if not os.path.exists(__db_path):
        os.mkdir(__db_path);
    full_db_path = __db_path+"/"+__db_name;
    with sql.connect(full_db_path) as conn:
        df.to_sql(table_name, conn, if_exists="replace");

# 获取股票数据
def get_stock_info_dataFrame():
    # 新建空DataFrame
    stock_info = pd.DataFrame(); 
    # 循环获取上海数据
    stock_type_of_sh_list = ["主板A股","主板B股","科创板"];
    for stock_type in stock_type_of_sh_list:
        temp_stock_info = ak.stock_info_sh_name_code(stock_type);
        temp_stock_info = pd.DataFrame(temp_stock_info,columns=["COMPANY_ABBR","COMPANY_CODE","LISTING_DATE"]);
        temp_stock_info = temp_stock_info.rename(columns={'COMPANY_ABBR':'NAME',"COMPANY_CODE":"CODE"});
        temp_stock_info.insert(0,"CODE",temp_stock_info.pop("CODE"));
        temp_stock_info.insert(0,"TYPE","SH");
        temp_stock_info.insert(len(temp_stock_info.columns),"UPDATE_TIME",time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()));
        print(f"add shanghai {stock_type},size:{len(temp_stock_info)}");
        stock_info = stock_info.append(temp_stock_info);

    # 循环获取深圳数据
    stock_type_of_sz_list = ["A股列表","AB股列表"]
    for stock_type in stock_type_of_sz_list:
        temp_stock_info = ak.stock_info_sz_name_code(stock_type);
        temp_stock_info = pd.DataFrame(temp_stock_info,columns=["A股代码","A股简称","A股上市日期"]);
        temp_stock_info = temp_stock_info.rename(columns={'A股代码':'CODE',"A股简称":"NAME","A股上市日期":"LISTING_DATE"});
        temp_stock_info.insert(0,"TYPE","SZ");
        temp_stock_info.insert(len(temp_stock_info.columns),"UPDATE_TIME",time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()));
        print(f"add shenzhen {stock_type},size:{len(temp_stock_info)}");
        # print(temp_stock_info)
        stock_info = stock_info.append(temp_stock_info);
    stock_info = stock_info.reset_index();
    return stock_info





if __name__=='__main__':
    print("in test main module");
    print(__db_path+"/"+__db_name);
    stock_info = get_stock_info_dataFrame();
    print(stock_info);
    replace_db(stock_info,"stock_info")
        