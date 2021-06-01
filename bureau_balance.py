# -*- coding: utf-8 -*-
"""
Created on Tue Jun  1 19:33:23 2021

@author: marks
"""


import pandas as pd

path='C:/Users/ntpu_metrics/Documents/Python Scripts/Datasets_ISLR/'
bureau_balance=pd.read_csv(path+'bureau_balance.csv')

############分別讀取bureau_balance.csv 中 數值欄位 與 類別欄位(會轉成虛擬變數欄位)
bureau_balance_num=bureau_balance[['MONTHS_BALANCE','SK_ID_BUREAU']]# 讀取bureau_balance.csv 中的數值欄位'MONTHS_BALANCE' 並保留Key'SK_ID_BUREAU'
bureau_balance_cate=bureau_balance[['STATUS','SK_ID_BUREAU']]# 讀取bureau_balance.csv 中的類別欄位'MONTHS_BALANCE' 並保留Key'SK_ID_BUREAU'
bureau_balance_cate_todummy=pd.get_dummies(bureau_balance_cate)# 將bureau_balance.csv 中的類別欄位 轉成虛擬變數欄位 並保留Key'SK_ID_BUREAU'

############計算bureau_balance.csv 中 數值欄位 與 虛擬變數欄位 的 平均值
bureau_balance_num_mean=bureau_balance_num.groupby('SK_ID_BUREAU').agg('mean')# 將bureau_balance.csv 中的數值欄位 以Key'SK_ID_BUREAU'分群 並計算平均數
bureau_balance_cate_todummy_mean=bureau_balance_cate_todummy.groupby('SK_ID_BUREAU').agg('mean')# 將bureau_balance.csv 中的類別欄位 轉成虛擬變數欄位 以Key'SK_ID_BUREAU'分群 並計算平均數

############將計算後的欄位合併
bureau_balance_mean=bureau_balance_num_mean.merge(bureau_balance_cate_todummy_mean,on='SK_ID_BUREAU')#將計算後的欄位合併
bureau_balance_mean.head()