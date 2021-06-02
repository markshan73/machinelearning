# -*- coding: utf-8 -*-
"""
Created on Tue Jun  1 19:33:23 2021

@author: marks
"""


import pandas as pd

path='C:/Users/ntpu_metrics/Documents/Python Scripts/Datasets_ISLR/'
bureau_balance=pd.read_csv(path+'bureau_balance.csv')
bureau=pd.read_csv(path+'bureau.csv')
application_train=pd.read_csv(path+'application_train.csv')
application_test=pd.read_csv(path+'application_test.csv')
############分別讀取bureau_balance.csv 中 數值欄位 與 類別欄位(會轉成虛擬變數欄位)
bureau_balance_num=bureau_balance[['SK_ID_BUREAU','MONTHS_BALANCE']]# 讀取bureau_balance.csv 中的數值欄位'MONTHS_BALANCE' 並保留Key'SK_ID_BUREAU'
bureau_balance_cate=bureau_balance[['SK_ID_BUREAU','STATUS']]# 讀取bureau_balance.csv 中的類別欄位'MONTHS_BALANCE' 並保留Key'SK_ID_BUREAU'
bureau_balance_cate_todummy=pd.get_dummies(bureau_balance_cate)# 將bureau_balance.csv 中的類別欄位 轉成虛擬變數欄位 並保留Key'SK_ID_BUREAU'

############計算bureau_balance.csv 中 數值欄位 與 虛擬變數欄位 的 平均值
bureau_balance_num_mean=bureau_balance_num.groupby('SK_ID_BUREAU').agg('mean')# 將bureau_balance.csv 中的數值欄位 以Key'SK_ID_BUREAU'分群 並計算平均數
bureau_balance_cate_todummy_mean=bureau_balance_cate_todummy.groupby('SK_ID_BUREAU').agg('mean')# 將bureau_balance.csv 中的類別欄位 轉成虛擬變數欄位 以Key'SK_ID_BUREAU'分群 並計算平均數

############將計算後的欄位合併
#bureau_balance_mean=bureau_balance_num_mean.merge(bureau_balance_cate_todummy_mean,on='SK_ID_BUREAU')#將計算後的欄位合併
#bureau_cate_todummy.head()

############分別讀取bureau.csv 中 數值欄位 與 類別欄位(會轉成虛擬變數欄位)
bureau_num=bureau[['SK_ID_CURR','SK_ID_BUREAU','DAYS_CREDIT','CREDIT_DAY_OVERDUE','DAYS_CREDIT_ENDDATE','DAYS_ENDDATE_FACT','AMT_CREDIT_MAX_OVERDUE','CNT_CREDIT_PROLONG','AMT_CREDIT_SUM','AMT_CREDIT_SUM_DEBT','AMT_CREDIT_SUM_LIMIT','AMT_CREDIT_SUM_OVERDUE','DAYS_CREDIT_UPDATE','AMT_ANNUITY']]# 讀取bureau.csv 中的數值欄位 並保留Key 'SK_ID_CURR'與'SK_ID_BUREAU' 
bureau_cate=bureau[['SK_ID_CURR','SK_ID_BUREAU','CREDIT_ACTIVE','CREDIT_CURRENCY','CREDIT_TYPE']]# 讀取bureau.csv 中的類別欄位'CCREDIT_ACTIVE','CREDIT_CURRENCY','CREDIT_TYPE' 並保留Key 'SK_ID_CURR'與'SK_ID_BUREAU' 
bureau_cate_todummy=pd.get_dummies(bureau_cate)# 將bureau_balance.csv 中的類別欄位 轉成虛擬變數欄位 並保留Key'SK_ID_BUREAU'

bureau_numAndbureau_balance_num_mean=bureau_num.merge(bureau_balance_num_mean,on='SK_ID_BUREAU')
bureau_cate_todummyAndbureau_balance_cate_todummy_mean=bureau_cate_todummy.merge(bureau_balance_cate_todummy_mean,on='SK_ID_BUREAU')
bureau_cate_todummyAndbureau_balance_cate_todummy_mean.to_csv()
############計算bureau.csv 中 數值欄位 與 虛擬變數欄位 的 平均值
bureau_num_mean=bureau_numAndbureau_balance_num_mean.groupby('application_train_num').agg('mean')# 將bureau_balance.csv 中的數值欄位 以Key'SK_ID_BUREAU'分群 並計算平均數
bureau_cate_todummy_mean=bureau_cate_todummyAndbureau_balance_cate_todummy_mean.groupby('SK_ID_CURR').agg('mean')# 將bureau_balance.csv 中的類別欄位 轉成虛擬變數欄位 以Key'SK_ID_BUREAU'分群 並計算平均數

############將計算後的欄位合併
#bureau_mean=bureau_num_mean.merge(bureau_cate_todummy_mean,on='SK_ID_BUREAU')#將計算後的欄位合併


############分別讀取application_train.csv 中 Boolean欄位 數值欄位 與 類別欄位(會轉成虛擬變數欄位)
application_train_boolean=application_train[['SK_ID_CURR','NAME_CONTRACT_TYPE',	'CODE_GENDER',	'FLAG_OWN_CAR',	'FLAG_OWN_REALTY',	'FLAG_MOBIL',	'FLAG_EMP_PHONE',	'FLAG_WORK_PHONE',	'FLAG_CONT_MOBILE',	'FLAG_PHONE',	'FLAG_EMAIL',	'REG_REGION_NOT_LIVE_REGION',	'REG_REGION_NOT_WORK_REGION',	'LIVE_REGION_NOT_WORK_REGION',	'REG_CITY_NOT_LIVE_CITY',	'REG_CITY_NOT_WORK_CITY',	'LIVE_CITY_NOT_WORK_CITY',	'FLAG_DOCUMENT_2',	'FLAG_DOCUMENT_3',	'FLAG_DOCUMENT_4',	'FLAG_DOCUMENT_5',	'FLAG_DOCUMENT_6',	'FLAG_DOCUMENT_7',	'FLAG_DOCUMENT_8',	'FLAG_DOCUMENT_9',	'FLAG_DOCUMENT_10',	'FLAG_DOCUMENT_11',	'FLAG_DOCUMENT_12',	'FLAG_DOCUMENT_13',	'FLAG_DOCUMENT_14',	'FLAG_DOCUMENT_15',	'FLAG_DOCUMENT_16',	'FLAG_DOCUMENT_17',	'FLAG_DOCUMENT_18',	'FLAG_DOCUMENT_19',	'FLAG_DOCUMENT_20',	'FLAG_DOCUMENT_21'
]]# 讀取application_train.csv 中的Boolean欄位 並保留Key 'SK_ID_CURR'
application_train_num=application_train[['SK_ID_CURR','CNT_CHILDREN',	'AMT_INCOME_TOTAL',	'AMT_CREDIT',	'AMT_ANNUITY',	'AMT_GOODS_PRICE',	'REGION_POPULATION_RELATIVE',	'DAYS_BIRTH',	'DAYS_EMPLOYED',	'DAYS_REGISTRATION',	'DAYS_ID_PUBLISH',	'OWN_CAR_AGE',	'CNT_FAM_MEMBERS',	'REGION_RATING_CLIENT',	'REGION_RATING_CLIENT_W_CITY',	'HOUR_APPR_PROCESS_START',	'EXT_SOURCE_1',	'EXT_SOURCE_2',	'EXT_SOURCE_3',	'APARTMENTS_AVG',	'BASEMENTAREA_AVG',	'YEARS_BEGINEXPLUATATION_AVG',	'YEARS_BUILD_AVG',	'COMMONAREA_AVG',	'ELEVATORS_AVG',	'ENTRANCES_AVG',	'FLOORSMAX_AVG',	'FLOORSMIN_AVG',	'LANDAREA_AVG',	'LIVINGAPARTMENTS_AVG',	'LIVINGAREA_AVG',	'NONLIVINGAPARTMENTS_AVG',	'NONLIVINGAREA_AVG',	'APARTMENTS_MODE',	'BASEMENTAREA_MODE',	'YEARS_BEGINEXPLUATATION_MODE',	'YEARS_BUILD_MODE',	'COMMONAREA_MODE',	'ELEVATORS_MODE',	'ENTRANCES_MODE',	'FLOORSMAX_MODE',	'FLOORSMIN_MODE',	'LANDAREA_MODE',	'LIVINGAPARTMENTS_MODE',	'LIVINGAREA_MODE',	'NONLIVINGAPARTMENTS_MODE',	'NONLIVINGAREA_MODE',	'APARTMENTS_MEDI',	'BASEMENTAREA_MEDI',	'YEARS_BEGINEXPLUATATION_MEDI',	'YEARS_BUILD_MEDI',	'COMMONAREA_MEDI',	'ELEVATORS_MEDI',	'ENTRANCES_MEDI',	'FLOORSMAX_MEDI',	'FLOORSMIN_MEDI',	'LANDAREA_MEDI',	'LIVINGAPARTMENTS_MEDI',	'LIVINGAREA_MEDI',	'NONLIVINGAPARTMENTS_MEDI',	'NONLIVINGAREA_MEDI',	'FONDKAPREMONT_MODE',	'HOUSETYPE_MODE',	'TOTALAREA_MODE',	'WALLSMATERIAL_MODE',	'EMERGENCYSTATE_MODE',	'OBS_30_CNT_SOCIAL_CIRCLE',	'DEF_30_CNT_SOCIAL_CIRCLE',	'OBS_60_CNT_SOCIAL_CIRCLE',	'DEF_60_CNT_SOCIAL_CIRCLE',	'DAYS_LAST_PHONE_CHANGE',	'AMT_REQ_CREDIT_BUREAU_HOUR',	'AMT_REQ_CREDIT_BUREAU_DAY',	'AMT_REQ_CREDIT_BUREAU_WEEK',	'AMT_REQ_CREDIT_BUREAU_MON',	'AMT_REQ_CREDIT_BUREAU_QRT',	'AMT_REQ_CREDIT_BUREAU_YEAR',
]]# 讀取application_train.csv 中的數值欄位 並保留Key 'SK_ID_CURR' 
application_train_cate=application_train[['SK_ID_CURR','NAME_TYPE_SUITE',	'NAME_INCOME_TYPE',	'NAME_EDUCATION_TYPE',	'NAME_FAMILY_STATUS',	'NAME_HOUSING_TYPE',	'OCCUPATION_TYPE',	'WEEKDAY_APPR_PROCESS_START',	'ORGANIZATION_TYPE'
]]# 讀取application_train.csv 中的類別欄位 並保留Key 'SK_ID_CURR'
application_train_cate_todummy=pd.get_dummies(application_train_cate)# 將application_train.csv 中的類別欄位 轉成虛擬變數欄位 並保留Key'SK_ID_CURR'
application_train_cate_todummy.to_csv('application_train_cate_todummy.csv')
application_train_booleanAndnum=application_train_boolean.merge(application_train_num,on='SK_ID_CURR')#將Boolean欄位與數值欄位合併
application_train_booleanAndnumAnddummy=application_train_booleanAndnum.merge(application_train_cate_todummy,on='SK_ID_CURR')#將Boolean、數值與虛擬變數欄位合併



############分別讀取application_test.csv 中 Boolean欄位 數值欄位 與 類別欄位(會轉成虛擬變數欄位)
application_test_boolean=application_test[['SK_ID_CURR','NAME_CONTRACT_TYPE',	'CODE_GENDER',	'FLAG_OWN_CAR',	'FLAG_OWN_REALTY',	'FLAG_MOBIL',	'FLAG_EMP_PHONE',	'FLAG_WORK_PHONE',	'FLAG_CONT_MOBILE',	'FLAG_PHONE',	'FLAG_EMAIL',	'REG_REGION_NOT_LIVE_REGION',	'REG_REGION_NOT_WORK_REGION',	'LIVE_REGION_NOT_WORK_REGION',	'REG_CITY_NOT_LIVE_CITY',	'REG_CITY_NOT_WORK_CITY',	'LIVE_CITY_NOT_WORK_CITY',	'FLAG_DOCUMENT_2',	'FLAG_DOCUMENT_3',	'FLAG_DOCUMENT_4',	'FLAG_DOCUMENT_5',	'FLAG_DOCUMENT_6',	'FLAG_DOCUMENT_7',	'FLAG_DOCUMENT_8',	'FLAG_DOCUMENT_9',	'FLAG_DOCUMENT_10',	'FLAG_DOCUMENT_11',	'FLAG_DOCUMENT_12',	'FLAG_DOCUMENT_13',	'FLAG_DOCUMENT_14',	'FLAG_DOCUMENT_15',	'FLAG_DOCUMENT_16',	'FLAG_DOCUMENT_17',	'FLAG_DOCUMENT_18',	'FLAG_DOCUMENT_19',	'FLAG_DOCUMENT_20',	'FLAG_DOCUMENT_21'
]]# 讀取application_test.csv 中的Boolean欄位 並保留Key 'SK_ID_CURR'
application_test_num=application_test[['SK_ID_CURR','CNT_CHILDREN',	'AMT_INCOME_TOTAL',	'AMT_CREDIT',	'AMT_ANNUITY',	'AMT_GOODS_PRICE',	'REGION_POPULATION_RELATIVE',	'DAYS_BIRTH',	'DAYS_EMPLOYED',	'DAYS_REGISTRATION',	'DAYS_ID_PUBLISH',	'OWN_CAR_AGE',	'CNT_FAM_MEMBERS',	'REGION_RATING_CLIENT',	'REGION_RATING_CLIENT_W_CITY',	'HOUR_APPR_PROCESS_START',	'EXT_SOURCE_1',	'EXT_SOURCE_2',	'EXT_SOURCE_3',	'APARTMENTS_AVG',	'BASEMENTAREA_AVG',	'YEARS_BEGINEXPLUATATION_AVG',	'YEARS_BUILD_AVG',	'COMMONAREA_AVG',	'ELEVATORS_AVG',	'ENTRANCES_AVG',	'FLOORSMAX_AVG',	'FLOORSMIN_AVG',	'LANDAREA_AVG',	'LIVINGAPARTMENTS_AVG',	'LIVINGAREA_AVG',	'NONLIVINGAPARTMENTS_AVG',	'NONLIVINGAREA_AVG',	'APARTMENTS_MODE',	'BASEMENTAREA_MODE',	'YEARS_BEGINEXPLUATATION_MODE',	'YEARS_BUILD_MODE',	'COMMONAREA_MODE',	'ELEVATORS_MODE',	'ENTRANCES_MODE',	'FLOORSMAX_MODE',	'FLOORSMIN_MODE',	'LANDAREA_MODE',	'LIVINGAPARTMENTS_MODE',	'LIVINGAREA_MODE',	'NONLIVINGAPARTMENTS_MODE',	'NONLIVINGAREA_MODE',	'APARTMENTS_MEDI',	'BASEMENTAREA_MEDI',	'YEARS_BEGINEXPLUATATION_MEDI',	'YEARS_BUILD_MEDI',	'COMMONAREA_MEDI',	'ELEVATORS_MEDI',	'ENTRANCES_MEDI',	'FLOORSMAX_MEDI',	'FLOORSMIN_MEDI',	'LANDAREA_MEDI',	'LIVINGAPARTMENTS_MEDI',	'LIVINGAREA_MEDI',	'NONLIVINGAPARTMENTS_MEDI',	'NONLIVINGAREA_MEDI',	'FONDKAPREMONT_MODE',	'HOUSETYPE_MODE',	'TOTALAREA_MODE',	'WALLSMATERIAL_MODE',	'EMERGENCYSTATE_MODE',	'OBS_30_CNT_SOCIAL_CIRCLE',	'DEF_30_CNT_SOCIAL_CIRCLE',	'OBS_60_CNT_SOCIAL_CIRCLE',	'DEF_60_CNT_SOCIAL_CIRCLE',	'DAYS_LAST_PHONE_CHANGE',	'AMT_REQ_CREDIT_BUREAU_HOUR',	'AMT_REQ_CREDIT_BUREAU_DAY',	'AMT_REQ_CREDIT_BUREAU_WEEK',	'AMT_REQ_CREDIT_BUREAU_MON',	'AMT_REQ_CREDIT_BUREAU_QRT',	'AMT_REQ_CREDIT_BUREAU_YEAR',
]]# 讀取application_test.csv 中的數值欄位 並保留Key 'SK_ID_CURR' 
application_test_cate=application_test[['SK_ID_CURR','NAME_TYPE_SUITE',	'NAME_INCOME_TYPE',	'NAME_EDUCATION_TYPE',	'NAME_FAMILY_STATUS',	'NAME_HOUSING_TYPE',	'OCCUPATION_TYPE',	'WEEKDAY_APPR_PROCESS_START',	'ORGANIZATION_TYPE'
]]# 讀取application_test.csv 中的類別欄位 並保留Key 'SK_ID_CURR'
application_test_cate_todummy=pd.get_dummies(application_test_cate)# 將application_test.csv 中的類別欄位 轉成虛擬變數欄位 並保留Key'SK_ID_CURR'
application_test_cate_todummy.to_csv("application_test_cate_todummy.csv")
application_test_booleanAndnum=application_test_boolean.merge(application_test_num,on='SK_ID_CURR')#將Boolean欄位與數值欄位合併
application_test_booleanAndnumAnddummy=application_test_booleanAndnum.merge(application_test_cate_todummy,on='SK_ID_CURR')#將Boolean、數值與虛擬變數欄位合併

#EOF
