# -*- coding: utf-8 -*-
"""
Created on Sun Jun  6 20:13:43 2021

@author: marks
"""



############################################################################################
#                      匯入資料 
############################################################################################
import numpy as np
import pandas as pd
path='C:/Users/ntpu_metrics/Documents/Python Scripts/Datasets_ISLR/'
bureau_balance=pd.read_csv(path+'bureau_balance.csv')
bureau=pd.read_csv(path+'bureau.csv')
installments_payments=pd.read_csv(path+'installments_payments.csv')
POS_CASH_balance=pd.read_csv(path+'POS_CASH_balance.csv')
credit_card_balance=pd.read_csv(path+'credit_card_balance.csv')
previous_application=pd.read_csv(path+'previous_application.csv')
application_train=pd.read_csv(path+'application_train.csv')
application_test=pd.read_csv(path+'application_test.csv')

############################################################################################
#                      刪除缺失值過多(47%)欄位 
############################################################################################
atmv = application_train.isnull().sum().sort_values()
atmv = atmv[atmv>0]
atmv_rate = atmv/len(application_train)
atmv_df = pd.DataFrame({'atmv':atmv, 'atmv_rate':atmv_rate})
print('number of features with more than 47% missing', len(atmv_rate[atmv_rate>0.47]))
atmv_rate[atmv_rate> 0.47]
miss47=atmv_rate[atmv_rate> 0.47]
miss47indexlist1=miss47.index.tolist()
at = application_train.drop(columns=[col for col in application_train if col  in miss47indexlist1])#(307511,73)
atest = application_test.drop(columns=[col for col in application_train if col  in miss47indexlist1])#(48744,72)

bbmv = bureau_balance.isnull().sum().sort_values()
bbmv = bbmv[bbmv>0]
bbmv_rate = bbmv/len(bureau_balance)
bbmv_df = pd.DataFrame({'bbmv':bbmv, 'bbmv_rate':bbmv_rate})
print('number of features with more than 47% missing', len(bbmv_rate[bbmv_rate>0.47]))
bbmv_rate[bbmv_rate> 0.47]
bb= bureau_balance

bmv = bureau.isnull().sum().sort_values()
bmv = bmv[bmv>0]
bmv_rate = bmv/len(bureau)
bmv_df = pd.DataFrame({'bmv':bmv, 'bmv_rate':bmv_rate})
print('number of features with more than 47% missing', len(bmv_rate[bmv_rate>0.47]))
bmv_rate[bmv_rate> 0.47]
miss47=bmv_rate[bmv_rate> 0.47]
miss47indexlist=miss47.index.tolist()
b = bureau.drop(columns=[col for col in bureau if col  in miss47indexlist])

imv = installments_payments.isnull().sum().sort_values()
imv = imv[imv>0]
imv_rate = imv/len(installments_payments)
imv_df = pd.DataFrame({'imv':imv, 'imv_rate':imv_rate})
print('number of features with more than 47% missing', len(imv_rate[imv_rate>0.47]))
imv_rate[imv_rate> 0.47]
i= installments_payments

PCmv = POS_CASH_balance.isnull().sum().sort_values()
PCmv = PCmv[PCmv>0]
PCmv_rate = PCmv/len(POS_CASH_balance)
PCmv_df = pd.DataFrame({'PCmv':PCmv, 'PCmv_rate':PCmv_rate})
print('number of features with more than 47% missing', len(PCmv_rate[PCmv_rate>0.47]))
PCmv_rate[PCmv_rate> 0.47]
PC = POS_CASH_balance

ccmv = credit_card_balance.isnull().sum().sort_values()
ccmv = ccmv[ccmv>0]
ccmv_rate = ccmv/len(credit_card_balance)
ccmv_df = pd.DataFrame({'ccmv':ccmv, 'ccmv_rate':ccmv_rate})
print('number of features with more than 47% missing', len(ccmv_rate[ccmv_rate>0.47]))
ccmv_rate[ccmv_rate> 0.47]
cc = credit_card_balance

pmv =previous_application.isnull().sum().sort_values()
pmv = pmv[pmv>0]
pmv_rate = pmv/len(previous_application)
pmv_df = pd.DataFrame({'pmv':pmv, 'pmv_rate':pmv_rate})
print('number of features with more than 47% missing', len(pmv_rate[pmv_rate>0.47]))
pmv_rate[pmv_rate> 0.47]
miss47=pmv_rate[pmv_rate> 0.47]
miss47indexlist=miss47.index.tolist()
p = previous_application.drop(columns=[col for col in previous_application if col  in miss47indexlist])

############################################################################################
#                      數值欄位 
############################################################################################
############讀取bureau_balance.csv 中數值欄位 
bb_num=bureau_balance[['SK_ID_BUREAU','MONTHS_BALANCE']]# 讀取bureau_balance.csv 中的數值欄位'MONTHS_BALANCE' 並保留Key'SK_ID_BUREAU'
bb_num.rename(columns={'MONTHS_BALANCE': 'MONTHS_BALANCE_bureau_balance'}, inplace=True)
############計算bureau_balance.csv 中數值欄位的最小值(前幾個月)
bb_num_aggregations = {'MONTHS_BALANCE_bureau_balance': ['min']}
bb_num_agg=bb_num.groupby('SK_ID_BUREAU').agg(bb_num_aggregations)# 將bureau_balance.csv 中的數值欄位 以Key'SK_ID_BUREAU'分群 並計算最小值
############讀取bureau.csv 中數值欄位
b_num=bureau[['SK_ID_CURR','SK_ID_BUREAU','DAYS_CREDIT','CREDIT_DAY_OVERDUE','DAYS_CREDIT_ENDDATE','DAYS_ENDDATE_FACT','CNT_CREDIT_PROLONG','AMT_CREDIT_SUM','AMT_CREDIT_SUM_DEBT','AMT_CREDIT_SUM_LIMIT','AMT_CREDIT_SUM_OVERDUE','DAYS_CREDIT_UPDATE']]# 讀取bureau.csv 中的數值欄位 並保留Key 'SK_ID_CURR'與'SK_ID_BUREAU' 

#&&&&&&&&&&&將bureau.csv 中數值欄位與bureau_balance.csv 中數值欄位合併
b_num=b_num.merge(bb_num_agg,on='SK_ID_BUREAU',how='left')# 將bureau.csv的數值欄位與bureau_balanc.csv的數值欄位合併 保留bureau_num的Key

############計算bureau.csv 中數值欄位的平均值
b_num_aggregations = {'DAYS_CREDIT':['mean'],'CREDIT_DAY_OVERDUE':['mean'],'DAYS_CREDIT_ENDDATE':['mean'],'DAYS_ENDDATE_FACT':['mean'],'CNT_CREDIT_PROLONG':['mean'],'AMT_CREDIT_SUM':['mean'],'AMT_CREDIT_SUM_DEBT':['mean'],'AMT_CREDIT_SUM_LIMIT':['mean'],'AMT_CREDIT_SUM_OVERDUE':['mean'],'DAYS_CREDIT_UPDATE':['mean']}
b_num_agg=b_num.groupby('SK_ID_CURR').agg(b_num_aggregations)# 將bureau.csv 中的數值欄位 以Key'SK_ID_BUREAU'分群 並計算平均數
b_num_agg.columns = ["_".join(x) for x in b_num_agg.columns.ravel()]
b_num_agg.reset_index(level='SK_ID_CURR', col_level=1)
############讀取POS_CASH_balance.csv 中數值欄位 
PC_num=PC[['SK_ID_PREV','SK_ID_CURR','MONTHS_BALANCE','CNT_INSTALMENT','CNT_INSTALMENT_FUTURE','SK_DPD','SK_DPD_DEF']]# 讀取POS_CASH_balance.csv 中的數值欄位'MONTHS_BALANCE' 並保留Key'SK_ID_PREV' 'SK_ID_CURR'
pcb_num_aggregations = {'MONTHS_BALANCE':['mean'],'CNT_INSTALMENT':['mean'],'CNT_INSTALMENT_FUTURE':['mean'],'SK_DPD':['mean'],'SK_DPD_DEF':['mean']}#計算POS_CASH_balance.csv 中數值欄位的平均值
PC_num_agg=PC_num.groupby('SK_ID_CURR').agg(pcb_num_aggregations)# 將POS_CASH_balance.csv 中的數值欄位 以Key'SK_ID_PREV'分群 並計算平均數
PC_num_agg.columns = ["_".join(x) for x in PC_num_agg.columns.ravel()]
PC_num_agg.reset_index(level='SK_ID_CURR', col_level=1)
############讀取installments_payments.csv 中數值欄位 
i_num=i[['SK_ID_PREV','SK_ID_CURR','NUM_INSTALMENT_VERSION','NUM_INSTALMENT_NUMBER','DAYS_INSTALMENT','DAYS_ENTRY_PAYMENT','AMT_INSTALMENT','AMT_PAYMENT']]# 讀取installments_payments.csv 中的數值欄位 並保留Key SK_ID_PREV' 'SK_ID_CURR'
############計算installments_payments.csv 中數值欄位的平均值
i_num_aggregations = {'NUM_INSTALMENT_VERSION':['mean'],'NUM_INSTALMENT_NUMBER':['mean'],'DAYS_INSTALMENT':['mean'],'DAYS_ENTRY_PAYMENT':['mean'],'AMT_INSTALMENT':['mean'],'AMT_PAYMENT':['mean']}
i_num_agg=i_num.groupby('SK_ID_CURR').agg(i_num_aggregations)# 將installments_payments.csv 中的數值欄位 以Key'SK_ID_PREV'分群 並計算平均數
i_num_agg.columns = ["_".join(x) for x in i_num_agg.columns.ravel()]
i_num_agg.reset_index(level='SK_ID_CURR', col_level=1)
############讀取credit_card_balance.csv 中數值欄位 
cc_num=cc[['SK_ID_PREV','SK_ID_CURR','MONTHS_BALANCE','AMT_BALANCE','AMT_CREDIT_LIMIT_ACTUAL','AMT_DRAWINGS_ATM_CURRENT','AMT_DRAWINGS_CURRENT','AMT_DRAWINGS_OTHER_CURRENT','AMT_DRAWINGS_POS_CURRENT','AMT_INST_MIN_REGULARITY','AMT_PAYMENT_CURRENT','AMT_PAYMENT_TOTAL_CURRENT','AMT_RECEIVABLE_PRINCIPAL','AMT_RECIVABLE','AMT_TOTAL_RECEIVABLE','CNT_DRAWINGS_ATM_CURRENT','CNT_DRAWINGS_CURRENT','CNT_DRAWINGS_OTHER_CURRENT','CNT_DRAWINGS_POS_CURRENT','CNT_INSTALMENT_MATURE_CUM','SK_DPD','SK_DPD_DEF']]# 讀取credit_card_balance.csv 中的數值欄位 並保留Key'SK_ID_PREV' 'SK_ID_CURR'
############計算credit_card_balance.csv 中數值欄位的平均值
cc_num_aggregations = {'MONTHS_BALANCE': ['mean'],'AMT_BALANCE': ['mean'],'AMT_CREDIT_LIMIT_ACTUAL': ['mean'],'AMT_DRAWINGS_ATM_CURRENT': ['mean'],'AMT_DRAWINGS_CURRENT': ['mean'],'AMT_DRAWINGS_OTHER_CURRENT': ['mean'],'AMT_DRAWINGS_POS_CURRENT': ['mean'],'AMT_INST_MIN_REGULARITY': ['mean'],'AMT_PAYMENT_CURRENT': ['mean'],'AMT_PAYMENT_TOTAL_CURRENT': ['mean'],'AMT_RECEIVABLE_PRINCIPAL': ['mean'],'AMT_RECIVABLE': ['mean'],'AMT_TOTAL_RECEIVABLE': ['mean'],'CNT_DRAWINGS_ATM_CURRENT': ['mean'],'CNT_DRAWINGS_CURRENT': ['mean'],'CNT_DRAWINGS_OTHER_CURRENT': ['mean'],'CNT_DRAWINGS_POS_CURRENT': ['mean'],'CNT_INSTALMENT_MATURE_CUM': ['mean'],'SK_DPD': ['mean'],'SK_DPD_DEF': ['mean']}
cc_num_agg=cc_num.groupby('SK_ID_CURR').agg(cc_num_aggregations)# 將credit_card_balance.csv 中的數值欄位 以Key'SK_ID_PREV'分群 並計算平均數
cc_num_agg.columns = ["_".join(x) for x in cc_num_agg.columns.ravel()]
cc_num_agg.reset_index(level='SK_ID_CURR', col_level=1)
############分別讀取previous_application.csv 中 Boolean欄位 數值欄位 與 類別欄位(會轉成虛擬變數欄位)
p_num=previous_application[['SK_ID_PREV','SK_ID_CURR','AMT_ANNUITY','AMT_APPLICATION','AMT_CREDIT','AMT_GOODS_PRICE','HOUR_APPR_PROCESS_START','DAYS_DECISION','SELLERPLACE_AREA','CNT_PAYMENT','DAYS_FIRST_DRAWING','DAYS_FIRST_DUE','DAYS_LAST_DUE_1ST_VERSION','DAYS_LAST_DUE','DAYS_TERMINATION']]# 讀取previous_application.csv 中的數值欄位 並保留Key'SK_ID_PREV' 'SK_ID_CURR'()
############計算previous_application.csv 中數值欄位的平均值
p_num_aggregations={'AMT_ANNUITY':['mean'],'AMT_APPLICATION':['mean'],'AMT_CREDIT':['mean'],'AMT_GOODS_PRICE':['mean'],'HOUR_APPR_PROCESS_START':['mean'],'DAYS_DECISION':['mean'],'SELLERPLACE_AREA':['mean'],'CNT_PAYMENT':['mean'],'DAYS_FIRST_DRAWING':['mean'],'DAYS_FIRST_DUE':['mean'],'DAYS_LAST_DUE_1ST_VERSION':['mean'],'DAYS_LAST_DUE':['mean'],'DAYS_TERMINATION':['mean']}
p_num_agg=p_num.groupby('SK_ID_CURR').agg(p_num_aggregations)# 將previous_application.csv 中的數值欄位 以Key'SK_ID_previous_application'分群 並計算平均數
p_num_agg.columns = ["_".join(x) for x in p_num_agg.columns.ravel()]
p_num_agg.reset_index(level='SK_ID_CURR', col_level=1)
############讀取application_train.csv 中 數值欄位              
at_num=at[['SK_ID_CURR','DAYS_REGISTRATION','HOUR_APPR_PROCESS_START','AMT_REQ_CREDIT_BUREAU_WEEK','DEF_60_CNT_SOCIAL_CIRCLE','EXT_SOURCE_3','CNT_CHILDREN','AMT_REQ_CREDIT_BUREAU_QRT','CNT_FAM_MEMBERS','DAYS_ID_PUBLISH','DEF_30_CNT_SOCIAL_CIRCLE','REGION_POPULATION_RELATIVE','EXT_SOURCE_2','AMT_REQ_CREDIT_BUREAU_YEAR','AMT_ANNUITY','OBS_30_CNT_SOCIAL_CIRCLE','DAYS_LAST_PHONE_CHANGE','AMT_CREDIT','DAYS_EMPLOYED','REGION_RATING_CLIENT_W_CITY','AMT_REQ_CREDIT_BUREAU_DAY','AMT_GOODS_PRICE','AMT_REQ_CREDIT_BUREAU_HOUR','OBS_60_CNT_SOCIAL_CIRCLE','REGION_RATING_CLIENT','DAYS_BIRTH','AMT_REQ_CREDIT_BUREAU_MON','AMT_INCOME_TOTAL']]# 讀取application_train.csv 中的數值欄位 並保留Key 'SK_ID_CURR' (307511,28)
#&&&&&&&&&&&將application_train.csv 中 數值欄位與bureau_num_agg中欄位 合併
at_num=at_num.merge(b_num_agg,on='SK_ID_CURR',how='left')# 將bureau.csv的數值欄位與bureau_num_mean合併(307511,38)
#&&&&&&&&&&&將application_train.csv 中 數值欄位與POS_CASH_balance_num_agg中欄位 合併
at_num=at_num.merge(PC_num_agg,on='SK_ID_CURR',how='left')# 將bureau.csv的數值欄位與POS_CASH_balance_num合併(307511,82)
#&&&&&&&&&&&將at.csv 中 數值欄位與installments_payments_num_agg中欄位 合併
at_num=at_num.merge(i_num_agg,on='SK_ID_CURR',how='left')# 將bureau.csv的數值欄位與installments_payments_mean合併(307511,82)
#&&&&&&&&&&&將at.csv 中 數值欄位與credit_card_balance_num_agg中欄位 合併
at_num=at_num.merge(cc_num_agg,  on='SK_ID_CURR',how='left')# 將bureau.csv的數值欄位與credit_card_balance_mean合併(307511,69)
#&&&&&&&&&&&將at.csv 中 數值欄位與previous_application_num_agg中欄位 合併
at_num=at_num.merge(p_num_agg, on='SK_ID_CURR',how='left')# 將bureau.csv的數值欄位與previous_application_num_mean合併(307511,82)

atest_num=atest[['SK_ID_CURR','DAYS_REGISTRATION','HOUR_APPR_PROCESS_START','AMT_REQ_CREDIT_BUREAU_WEEK','DEF_60_CNT_SOCIAL_CIRCLE','EXT_SOURCE_3','CNT_CHILDREN','AMT_REQ_CREDIT_BUREAU_QRT','CNT_FAM_MEMBERS','DAYS_ID_PUBLISH','DEF_30_CNT_SOCIAL_CIRCLE','REGION_POPULATION_RELATIVE','EXT_SOURCE_2','AMT_REQ_CREDIT_BUREAU_YEAR','AMT_ANNUITY','OBS_30_CNT_SOCIAL_CIRCLE','DAYS_LAST_PHONE_CHANGE','AMT_CREDIT','DAYS_EMPLOYED','REGION_RATING_CLIENT_W_CITY','AMT_REQ_CREDIT_BUREAU_DAY','AMT_GOODS_PRICE','AMT_REQ_CREDIT_BUREAU_HOUR','OBS_60_CNT_SOCIAL_CIRCLE','REGION_RATING_CLIENT','DAYS_BIRTH','AMT_REQ_CREDIT_BUREAU_MON','AMT_INCOME_TOTAL']]# # 讀取application_test.csv 中的數值欄位 並保留Key 'SK_ID_CURR' (48744,28)

#&&&&&&&&&&&將application_train.csv 中 數值欄位與bureau_num_agg中欄位 合併
atest_num=atest_num.merge(b_num_agg,on='SK_ID_CURR',how='left')# 將bureau.csv的數值欄位與bureau_num_mean合併(48744,38)
#&&&&&&&&&&&將application_train.csv 中 數值欄位與POS_CASH_balance_num_agg中欄位 合併
atest_num=atest_num.merge(PC_num_agg,on='SK_ID_CURR',how='left')# 將bureau.csv的數值欄位與POS_CASH_balance_num合併(48744,43)
#&&&&&&&&&&&將atest_num.csv 中 數值欄位與installments_payments_num_agg中欄位 合併
atest_num=atest_num.merge(i_num_agg,on='SK_ID_CURR',how='left')# 將bureau.csv的數值欄位與installments_payments_mean合併(48744,49)
#&&&&&&&&&&&將atest_num.csv 中 數值欄位與credit_card_balance_num_agg中欄位 合併
atest_num=atest_num.merge(cc_num_agg,  on='SK_ID_CURR',how='left')# 將bureau.csv的數值欄位與credit_card_balance_mean合併(48744,69)
#&&&&&&&&&&&將atest_num.csv 中 數值欄位與previous_application_num_agg中欄位 合併
atest_num=atest_num.merge(p_num_agg, on='SK_ID_CURR',how='left')# 將bureau.csv的數值欄位與previous_application_num_mean合併(48744,82)

############################################################################################
#                      刪除離群值(回填nan)
############################################################################################
def replace_outliers_with_nan(df, stds):#帶入dataframe、以多少個標準差計算離群值
    return df.mask(df.sub(df.mean()).div(df.std()).abs().gt(stds))# 將滿足{[(df減df平均數)/df標準差]取絕對值 大於 多少個標準差}的資料mask
at_num=replace_outliers_with_nan(at_num,3)#帶入application_train.csv 中數值欄位、以3個標準差計算離群值
atest_num=replace_outliers_with_nan(atest_num,3)#帶入application_test.csv 中數值欄位、以3個標準差計算離群值

############################################################################################
#                      類別欄位 
############################################################################################
############讀取bureau_balance.csv 中類別欄位 
bb_cate=bb[['SK_ID_BUREAU','STATUS']]# 讀取bureau_balance.csv 中的類別欄位'MONTHS_BALANCE' 並保留Key'SK_ID_BUREAU'
bb_cate=pd.get_dummies(bb_cate)
############計算bureau_balance.csv 中類別欄位的平均值(狀態) bb_aggregations = {'MONTHS_BALANCE': ['min', 'max', 'size']}
bb_cate_agg=bb_cate.groupby('SK_ID_BUREAU').agg('mean')# 將bureau_balance.csv 中的類別欄位 以Key'SK_ID_BUREAU'分群 並計算最小值
############讀取bureau.csv 中類別欄位
b_cate=b[['SK_ID_CURR','SK_ID_BUREAU','CREDIT_ACTIVE','CREDIT_CURRENCY','CREDIT_TYPE']]# 讀取bureau.csv 中的類別欄位 並保留Key 'SK_ID_CURR'與'SK_ID_BUREAU' 
b_cate=pd.get_dummies(b_cate)
#&&&&&&&&&&&將bureau.csv 中類別欄位與bureau_balance.csv 中類別欄位合併
b_cate=b_cate.merge(bb_cate_agg,on='SK_ID_BUREAU',how='left')# 將bureau.csv的類別欄位與bureau_balanc.csv的類別欄位合併 保留bureau_cate的Key
############計算bureau.csv 中類別欄位的平均值與加總
b_cate_agg=b_cate.groupby('SK_ID_CURR').agg('mean')# 將bureau.csv 中的類別欄位 以Key'SK_ID_BUREAU'分群 並計算平均數
#b_cate_agg.columns = ["_".join(x) for x in b_cate_agg.columns.ravel()]
#b_cate_agg.reset_index(level='SK_ID_CURR', col_level=1)
############讀取POS_CASH_balance.csv 中類別欄位 
PC_cate=PC[['SK_ID_CURR','NAME_CONTRACT_STATUS']]# 讀取POS_CASH_balance.csv 中的類別欄位'MONTHS_BALANCE' 並保留Key'SK_ID_PREV' 'SK_ID_CURR'
PC_cate=pd.get_dummies(PC_cate)
############計算POS_CASH_balance.csv 中類別欄位的平均值與加總
PC_cate_agg=PC_cate.groupby('SK_ID_CURR').agg('mean')# 將POS_CASH_balance.csv 中的類別欄位 以Key'SK_ID_PREV'分群 並計算平均數
#PC_cate_agg.columns = ["_".join(x) for x in PC_cate_agg.columns.ravel()]
#PC_cate_agg.reset_index(level='SK_ID_CURR', col_level=1)
############讀取credit_card_balance.csv 中類別欄位 
cc_cate=cc[['SK_ID_CURR','NAME_CONTRACT_STATUS']]# 讀取credit_card_balance.csv 中的類別欄位 並保留Key'SK_ID_PREV' 'SK_ID_CURR'
cc_cate=pd.get_dummies(cc_cate)
############計算credit_card_balance.csv 中類別欄位的平均值與加總
cc_cate_agg=cc_cate.groupby('SK_ID_CURR').agg('mean')# 將credit_card_balance.csv 中的類別欄位 以Key'SK_ID_PREV'分群 並計算平均數
#cc_cate_agg.columns = ["_".join(x) for x in cc_cate_agg.columns.ravel()]
#cc_cate_agg.reset_index(level='SK_ID_CURR', col_level=1)
############分別讀取previous_application.csv 中 Boolean欄位 類別欄位 與 類別欄位(會轉成虛擬變數欄位)
p_cate=previous_application[['SK_ID_PREV','SK_ID_CURR','NAME_CONTRACT_TYPE','WEEKDAY_APPR_PROCESS_START','NAME_CASH_LOAN_PURPOSE','NAME_CONTRACT_STATUS','NAME_PAYMENT_TYPE','CODE_REJECT_REASON','NAME_CLIENT_TYPE','NAME_GOODS_CATEGORY','NAME_PORTFOLIO','NAME_PRODUCT_TYPE','CHANNEL_TYPE','NAME_SELLER_INDUSTRY','NAME_YIELD_GROUP','PRODUCT_COMBINATION']]# 讀取previous_application.csv 中的類別欄位 並保留Key'SK_ID_PREV' 'SK_ID_CURR'()
p_cate=pd.get_dummies(p_cate)
############計算previous_application.csv 中類別欄位的平均值與加總
p_cate_agg=p_cate.groupby('SK_ID_CURR').agg('mean')# 將previous_application.csv 中的類別欄位 以Key'SK_ID_previous_application'分群 並計算平均數
#p_cate_agg.columns = ["_".join(x) for x in p_cate_agg.columns.ravel()]
#p_cate_agg.reset_index(level='SK_ID_CURR', col_level=1)
############讀取application_train.csv 中 類別欄位         
at_cate=at[['SK_ID_CURR','NAME_TYPE_SUITE',	'NAME_INCOME_TYPE',	'NAME_EDUCATION_TYPE',	'NAME_FAMILY_STATUS',	'NAME_HOUSING_TYPE',	'OCCUPATION_TYPE',	'WEEKDAY_APPR_PROCESS_START',	'ORGANIZATION_TYPE']]#(307511,9)
at_cate=pd.get_dummies(at_cate)#(307511,116)
#&&&&&&&&&&&將application_train.csv 中 類別欄位與bureau_cate_agg中欄位 合併
at_cate=at_cate.merge(b_cate_agg,on='SK_ID_CURR',how='left')# 將bureau.csv的類別欄位與bureau_cate_mean合併(307511,140)
#&&&&&&&&&&&將application_train.csv 中 類別欄位與POS_CASH_balance_cate_agg中欄位 合併
at_cate=at_cate.merge(PC_cate_agg,on='SK_ID_CURR',how='left')# 將bureau.csv的類別欄位與POS_CASH_balance_cate合併(307511,157)
#&&&&&&&&&&&將at.csv 中 類別欄位與credit_card_balance_cate_agg中欄位 合併
at_cate=at_cate.merge(cc_cate_agg,  on='SK_ID_CURR',how='left')# 將bureau.csv的類別欄位與credit_card_balance_mean合併(307511,164)
#&&&&&&&&&&&將at.csv 中 類別欄位與previous_application_cate_agg中欄位 合併
at_cate=at_cate.merge(p_cate_agg, on='SK_ID_CURR',how='left')# 將bureau.csv的類別欄位與previous_application_cate_mean合併(307511,299)

atest_cate=atest[['SK_ID_CURR','NAME_TYPE_SUITE',	'NAME_INCOME_TYPE',	'NAME_EDUCATION_TYPE',	'NAME_FAMILY_STATUS',	'NAME_HOUSING_TYPE',	'OCCUPATION_TYPE',	'WEEKDAY_APPR_PROCESS_START',	'ORGANIZATION_TYPE']]#(48744,9)
atest_cate=pd.get_dummies(atest_cate)#(48744,114)
#&&&&&&&&&&&將application_train.csv 中 類別欄位與bureau_cate_agg中欄位 合併
atest_cate=atest_cate.merge(b_cate_agg,on='SK_ID_CURR',how='left')# 將bureau.csv的類別欄位與bureau_cate_mean合併(48744,146)
#&&&&&&&&&&&將application_train.csv 中 類別欄位與POS_CASH_balance_cate_agg中欄位 合併
atest_cate=atest_cate.merge(PC_cate_agg,on='SK_ID_CURR',how='left')# 將bureau.csv的類別欄位與POS_CASH_balance_cate合併(48744,155)
#&&&&&&&&&&&將at_cate.csv 中 類別欄位與credit_card_balance_cate_agg中欄位 合併
atest_cate=atest_cate.merge(cc_cate_agg,  on='SK_ID_CURR',how='left')# 將bureau.csv的類別欄位與credit_card_balance_mean合併(48744,162)
#&&&&&&&&&&&將at_cate.csv 中 類別欄位與previous_application_cate_agg中欄位 合併
atest_cate=atest_cate.merge(p_cate_agg, on='SK_ID_CURR',how='left')# 將bureau.csv的類別欄位與previous_application_cate_mean合併(48744,297)
############################################################################################
#                      布林欄位 
############################################################################################
############分別讀取previous_application.csv 中 Boolean欄位
p_boolean=p[['SK_ID_CURR','FLAG_LAST_APPL_PER_CONTRACT','NFLAG_LAST_APPL_IN_DAY','NFLAG_INSURED_ON_APPROVAL']]# 讀取previous_application.csv 中的Boolean欄位 並保留Key'SK_ID_PREV' 'SK_ID_CURR'(1670214,5)
p_boolean['FLAG_LAST_APPL_PER_CONTRACT'] = p_boolean['FLAG_LAST_APPL_PER_CONTRACT'].map({'Y': 1, 'N': 0})
p_boolean=p_boolean.groupby('SK_ID_CURR').agg('mean')
############讀取application_train.csv 中 布林欄位
at_boolean=at[['SK_ID_CURR','NAME_CONTRACT_TYPE',	'CODE_GENDER',	'FLAG_OWN_CAR',	'FLAG_OWN_REALTY',	'FLAG_MOBIL',	'FLAG_EMP_PHONE',	'FLAG_WORK_PHONE',	'FLAG_CONT_MOBILE',	'FLAG_PHONE',	'FLAG_EMAIL',	'REG_REGION_NOT_LIVE_REGION',	'REG_REGION_NOT_WORK_REGION',	'LIVE_REGION_NOT_WORK_REGION',	'REG_CITY_NOT_LIVE_CITY',	'REG_CITY_NOT_WORK_CITY',	'LIVE_CITY_NOT_WORK_CITY',	'FLAG_DOCUMENT_2',	'FLAG_DOCUMENT_3',	'FLAG_DOCUMENT_4',	'FLAG_DOCUMENT_5',	'FLAG_DOCUMENT_6',	'FLAG_DOCUMENT_7',	'FLAG_DOCUMENT_8',	'FLAG_DOCUMENT_9',	'FLAG_DOCUMENT_10',	'FLAG_DOCUMENT_11',	'FLAG_DOCUMENT_12',	'FLAG_DOCUMENT_13',	'FLAG_DOCUMENT_14',	'FLAG_DOCUMENT_15',	'FLAG_DOCUMENT_16',	'FLAG_DOCUMENT_17',	'FLAG_DOCUMENT_18',	'FLAG_DOCUMENT_19',	'FLAG_DOCUMENT_20',	'FLAG_DOCUMENT_21'
]]#(307511,37)

#&&&&&&&&&&&將application_train.csv 中布林欄位與previous_application_cate_agg中欄位合併
at_boolean=at_boolean.merge(p_boolean, on='SK_ID_CURR',how='left')# 將bureau.csv的類別欄位與previous_application_cate_mean合併(307511,40)

############讀取application_test.csv 中 布林欄位
atest_boolean=atest[['SK_ID_CURR','NAME_CONTRACT_TYPE',	'CODE_GENDER',	'FLAG_OWN_CAR',	'FLAG_OWN_REALTY',	'FLAG_MOBIL',	'FLAG_EMP_PHONE',	'FLAG_WORK_PHONE',	'FLAG_CONT_MOBILE',	'FLAG_PHONE',	'FLAG_EMAIL',	'REG_REGION_NOT_LIVE_REGION',	'REG_REGION_NOT_WORK_REGION',	'LIVE_REGION_NOT_WORK_REGION',	'REG_CITY_NOT_LIVE_CITY',	'REG_CITY_NOT_WORK_CITY',	'LIVE_CITY_NOT_WORK_CITY',	'FLAG_DOCUMENT_2',	'FLAG_DOCUMENT_3',	'FLAG_DOCUMENT_4',	'FLAG_DOCUMENT_5',	'FLAG_DOCUMENT_6',	'FLAG_DOCUMENT_7',	'FLAG_DOCUMENT_8',	'FLAG_DOCUMENT_9',	'FLAG_DOCUMENT_10',	'FLAG_DOCUMENT_11',	'FLAG_DOCUMENT_12',	'FLAG_DOCUMENT_13',	'FLAG_DOCUMENT_14',	'FLAG_DOCUMENT_15',	'FLAG_DOCUMENT_16',	'FLAG_DOCUMENT_17',	'FLAG_DOCUMENT_18',	'FLAG_DOCUMENT_19',	'FLAG_DOCUMENT_20',	'FLAG_DOCUMENT_21'
]]#(48744,37)
#&&&&&&&&&&&將application_test..csv 中布林欄位與previous_application_cate_agg中欄位合併
atest_boolean=atest_boolean.merge(p_boolean, on='SK_ID_CURR',how='left')# 將bureau.csv的類別欄位與previous_application_cate_mean合併(48744,40)

############################################################################################
#                      欄位合併/刪除有遺漏值的列/匯出.csv
############################################################################################
at_merge=at_num.merge(at_boolean, on='SK_ID_CURR',how='left')#(307511,121)
at_merge=at_merge.merge(at_cate, on='SK_ID_CURR',how='left')#(307511,419)
at_merge=at_merge.merge(application_train, on='SK_ID_CURR',how='left')#(307511,540)
at_merge.dropna(how='any', inplace=True)#(655,544) 刪除有遺漏值的列
atest_merge=atest_num.merge(atest_boolean, on='SK_ID_CURR',how='left')#(48744,121)
atest_merge=atest_merge.merge(atest_cate, on='SK_ID_CURR',how='left')#(48744,417)
atest_merge.dropna(how='any', inplace=True)#(7621,417) 刪除有遺漏值的列
#目前訓練集欄位
atcl=['SK_ID_CURR','DAYS_REGISTRATION_x','HOUR_APPR_PROCESS_START_x','AMT_REQ_CREDIT_BUREAU_WEEK_x','DEF_60_CNT_SOCIAL_CIRCLE_x','EXT_SOURCE_3_x','CNT_CHILDREN_x','AMT_REQ_CREDIT_BUREAU_QRT_x','CNT_FAM_MEMBERS_x','DAYS_ID_PUBLISH_x','DEF_30_CNT_SOCIAL_CIRCLE_x','REGION_POPULATION_RELATIVE_x','EXT_SOURCE_2_x','AMT_REQ_CREDIT_BUREAU_YEAR_x','AMT_ANNUITY_x','OBS_30_CNT_SOCIAL_CIRCLE_x','DAYS_LAST_PHONE_CHANGE_x','AMT_CREDIT_x','DAYS_EMPLOYED_x','REGION_RATING_CLIENT_W_CITY_x','AMT_REQ_CREDIT_BUREAU_DAY_x','AMT_GOODS_PRICE_x','AMT_REQ_CREDIT_BUREAU_HOUR_x','OBS_60_CNT_SOCIAL_CIRCLE_x','REGION_RATING_CLIENT_x','DAYS_BIRTH_x','AMT_REQ_CREDIT_BUREAU_MON_x','AMT_INCOME_TOTAL_x','DAYS_CREDIT_mean','CREDIT_DAY_OVERDUE_mean','DAYS_CREDIT_ENDDATE_mean','DAYS_ENDDATE_FACT_mean','CNT_CREDIT_PROLONG_mean','AMT_CREDIT_SUM_mean','AMT_CREDIT_SUM_DEBT_mean','AMT_CREDIT_SUM_LIMIT_mean','AMT_CREDIT_SUM_OVERDUE_mean','DAYS_CREDIT_UPDATE_mean','MONTHS_BALANCE_mean_x','CNT_INSTALMENT_mean','CNT_INSTALMENT_FUTURE_mean','SK_DPD_mean_x','SK_DPD_DEF_mean_x','NUM_INSTALMENT_VERSION_mean','NUM_INSTALMENT_NUMBER_mean','DAYS_INSTALMENT_mean','DAYS_ENTRY_PAYMENT_mean','AMT_INSTALMENT_mean','AMT_PAYMENT_mean','MONTHS_BALANCE_mean_y','AMT_BALANCE_mean','AMT_CREDIT_LIMIT_ACTUAL_mean','AMT_DRAWINGS_ATM_CURRENT_mean','AMT_DRAWINGS_CURRENT_mean','AMT_DRAWINGS_OTHER_CURRENT_mean','AMT_DRAWINGS_POS_CURRENT_mean','AMT_INST_MIN_REGULARITY_mean','AMT_PAYMENT_CURRENT_mean','AMT_PAYMENT_TOTAL_CURRENT_mean','AMT_RECEIVABLE_PRINCIPAL_mean','AMT_RECIVABLE_mean','AMT_TOTAL_RECEIVABLE_mean','CNT_DRAWINGS_ATM_CURRENT_mean','CNT_DRAWINGS_CURRENT_mean','CNT_DRAWINGS_OTHER_CURRENT_mean','CNT_DRAWINGS_POS_CURRENT_mean','CNT_INSTALMENT_MATURE_CUM_mean','SK_DPD_mean_y','SK_DPD_DEF_mean_y','AMT_ANNUITY_mean','AMT_APPLICATION_mean','AMT_CREDIT_mean','AMT_GOODS_PRICE_mean','HOUR_APPR_PROCESS_START_mean','DAYS_DECISION_mean','SELLERPLACE_AREA_mean','CNT_PAYMENT_mean','DAYS_FIRST_DRAWING_mean','DAYS_FIRST_DUE_mean','DAYS_LAST_DUE_1ST_VERSION_mean','DAYS_LAST_DUE_mean','DAYS_TERMINATION_mean','NAME_CONTRACT_TYPE_x','CODE_GENDER_x','FLAG_OWN_CAR_x','FLAG_OWN_REALTY_x','FLAG_MOBIL_x','FLAG_EMP_PHONE_x','FLAG_WORK_PHONE_x','FLAG_CONT_MOBILE_x','FLAG_PHONE_x','FLAG_EMAIL_x','REG_REGION_NOT_LIVE_REGION_x','REG_REGION_NOT_WORK_REGION_x','LIVE_REGION_NOT_WORK_REGION_x','REG_CITY_NOT_LIVE_CITY_x','REG_CITY_NOT_WORK_CITY_x','LIVE_CITY_NOT_WORK_CITY_x','FLAG_DOCUMENT_2_x','FLAG_DOCUMENT_3_x','FLAG_DOCUMENT_4_x','FLAG_DOCUMENT_5_x','FLAG_DOCUMENT_6_x','FLAG_DOCUMENT_7_x','FLAG_DOCUMENT_8_x','FLAG_DOCUMENT_9_x','FLAG_DOCUMENT_10_x','FLAG_DOCUMENT_11_x','FLAG_DOCUMENT_12_x','FLAG_DOCUMENT_13_x','FLAG_DOCUMENT_14_x','FLAG_DOCUMENT_15_x','FLAG_DOCUMENT_16_x','FLAG_DOCUMENT_17_x','FLAG_DOCUMENT_18_x','FLAG_DOCUMENT_19_x','FLAG_DOCUMENT_20_x','FLAG_DOCUMENT_21_x','FLAG_LAST_APPL_PER_CONTRACT','NFLAG_LAST_APPL_IN_DAY','NFLAG_INSURED_ON_APPROVAL','NAME_TYPE_SUITE_Children','NAME_TYPE_SUITE_Family','NAME_TYPE_SUITE_Group of people','NAME_TYPE_SUITE_Other_A','NAME_TYPE_SUITE_Other_B','NAME_TYPE_SUITE_Spouse, partner','NAME_TYPE_SUITE_Unaccompanied','NAME_INCOME_TYPE_Businessman','NAME_INCOME_TYPE_Commercial associate','NAME_INCOME_TYPE_Maternity leave','NAME_INCOME_TYPE_Pensioner','NAME_INCOME_TYPE_State servant','NAME_INCOME_TYPE_Student','NAME_INCOME_TYPE_Unemployed','NAME_INCOME_TYPE_Working','NAME_EDUCATION_TYPE_Academic degree','NAME_EDUCATION_TYPE_Higher education','NAME_EDUCATION_TYPE_Incomplete higher','NAME_EDUCATION_TYPE_Lower secondary','NAME_EDUCATION_TYPE_Secondary / secondary special','NAME_FAMILY_STATUS_Civil marriage','NAME_FAMILY_STATUS_Married','NAME_FAMILY_STATUS_Separated','NAME_FAMILY_STATUS_Single / not married','NAME_FAMILY_STATUS_Unknown','NAME_FAMILY_STATUS_Widow','NAME_HOUSING_TYPE_Co-op apartment','NAME_HOUSING_TYPE_House / apartment','NAME_HOUSING_TYPE_Municipal apartment','NAME_HOUSING_TYPE_Office apartment','NAME_HOUSING_TYPE_Rented apartment','NAME_HOUSING_TYPE_With parents','OCCUPATION_TYPE_Accountants','OCCUPATION_TYPE_Cleaning staff','OCCUPATION_TYPE_Cooking staff','OCCUPATION_TYPE_Core staff','OCCUPATION_TYPE_Drivers','OCCUPATION_TYPE_HR staff','OCCUPATION_TYPE_High skill tech staff','OCCUPATION_TYPE_IT staff','OCCUPATION_TYPE_Laborers','OCCUPATION_TYPE_Low-skill Laborers','OCCUPATION_TYPE_Managers','OCCUPATION_TYPE_Medicine staff','OCCUPATION_TYPE_Private service staff','OCCUPATION_TYPE_Realty agents','OCCUPATION_TYPE_Sales staff','OCCUPATION_TYPE_Secretaries','OCCUPATION_TYPE_Security staff','OCCUPATION_TYPE_Waiters/barmen staff','WEEKDAY_APPR_PROCESS_START_FRIDAY_x','WEEKDAY_APPR_PROCESS_START_MONDAY_x','WEEKDAY_APPR_PROCESS_START_SATURDAY_x','WEEKDAY_APPR_PROCESS_START_SUNDAY_x','WEEKDAY_APPR_PROCESS_START_THURSDAY_x','WEEKDAY_APPR_PROCESS_START_TUESDAY_x','WEEKDAY_APPR_PROCESS_START_WEDNESDAY_x','ORGANIZATION_TYPE_Advertising','ORGANIZATION_TYPE_Agriculture','ORGANIZATION_TYPE_Bank','ORGANIZATION_TYPE_Business Entity Type 1','ORGANIZATION_TYPE_Business Entity Type 2','ORGANIZATION_TYPE_Business Entity Type 3','ORGANIZATION_TYPE_Cleaning','ORGANIZATION_TYPE_Construction','ORGANIZATION_TYPE_Culture','ORGANIZATION_TYPE_Electricity','ORGANIZATION_TYPE_Emergency','ORGANIZATION_TYPE_Government','ORGANIZATION_TYPE_Hotel','ORGANIZATION_TYPE_Housing','ORGANIZATION_TYPE_Industry: type 1','ORGANIZATION_TYPE_Industry: type 10','ORGANIZATION_TYPE_Industry: type 11','ORGANIZATION_TYPE_Industry: type 12','ORGANIZATION_TYPE_Industry: type 13','ORGANIZATION_TYPE_Industry: type 2','ORGANIZATION_TYPE_Industry: type 3','ORGANIZATION_TYPE_Industry: type 4','ORGANIZATION_TYPE_Industry: type 5','ORGANIZATION_TYPE_Industry: type 6','ORGANIZATION_TYPE_Industry: type 7','ORGANIZATION_TYPE_Industry: type 8','ORGANIZATION_TYPE_Industry: type 9','ORGANIZATION_TYPE_Insurance','ORGANIZATION_TYPE_Kindergarten','ORGANIZATION_TYPE_Legal Services','ORGANIZATION_TYPE_Medicine','ORGANIZATION_TYPE_Military','ORGANIZATION_TYPE_Mobile','ORGANIZATION_TYPE_Other','ORGANIZATION_TYPE_Police','ORGANIZATION_TYPE_Postal','ORGANIZATION_TYPE_Realtor','ORGANIZATION_TYPE_Religion','ORGANIZATION_TYPE_Restaurant','ORGANIZATION_TYPE_School','ORGANIZATION_TYPE_Security','ORGANIZATION_TYPE_Security Ministries','ORGANIZATION_TYPE_Self-employed','ORGANIZATION_TYPE_Services','ORGANIZATION_TYPE_Telecom','ORGANIZATION_TYPE_Trade: type 1','ORGANIZATION_TYPE_Trade: type 2','ORGANIZATION_TYPE_Trade: type 3','ORGANIZATION_TYPE_Trade: type 4','ORGANIZATION_TYPE_Trade: type 5','ORGANIZATION_TYPE_Trade: type 6','ORGANIZATION_TYPE_Trade: type 7','ORGANIZATION_TYPE_Transport: type 1','ORGANIZATION_TYPE_Transport: type 2','ORGANIZATION_TYPE_Transport: type 3','ORGANIZATION_TYPE_Transport: type 4','ORGANIZATION_TYPE_University','ORGANIZATION_TYPE_XNA','SK_ID_BUREAU','CREDIT_ACTIVE_Active','CREDIT_ACTIVE_Bad debt','CREDIT_ACTIVE_Closed','CREDIT_ACTIVE_Sold','CREDIT_CURRENCY_currency 1','CREDIT_CURRENCY_currency 2','CREDIT_CURRENCY_currency 3','CREDIT_CURRENCY_currency 4','CREDIT_TYPE_Another type of loan','CREDIT_TYPE_Car loan','CREDIT_TYPE_Cash loan (non-earmarked)','CREDIT_TYPE_Consumer credit','CREDIT_TYPE_Credit card','CREDIT_TYPE_Interbank credit','CREDIT_TYPE_Loan for business development','CREDIT_TYPE_Loan for purchase of shares (margin lending)','CREDIT_TYPE_Loan for the purchase of equipment','CREDIT_TYPE_Loan for working capital replenishment','CREDIT_TYPE_Microloan','CREDIT_TYPE_Mobile operator loan','CREDIT_TYPE_Mortgage','CREDIT_TYPE_Real estate loan','CREDIT_TYPE_Unknown type of loan','STATUS_0','STATUS_1','STATUS_2','STATUS_3','STATUS_4','STATUS_5','STATUS_C','STATUS_X','NAME_CONTRACT_STATUS_Active_x','NAME_CONTRACT_STATUS_Amortized debt','NAME_CONTRACT_STATUS_Approved_x','NAME_CONTRACT_STATUS_Canceled_x','NAME_CONTRACT_STATUS_Completed_x','NAME_CONTRACT_STATUS_Demand_x','NAME_CONTRACT_STATUS_Returned to the store','NAME_CONTRACT_STATUS_Signed_x','NAME_CONTRACT_STATUS_XNA','NAME_CONTRACT_STATUS_Active_y','NAME_CONTRACT_STATUS_Approved_y','NAME_CONTRACT_STATUS_Completed_y','NAME_CONTRACT_STATUS_Demand_y','NAME_CONTRACT_STATUS_Refused_x','NAME_CONTRACT_STATUS_Sent proposal','NAME_CONTRACT_STATUS_Signed_y','SK_ID_PREV','NAME_CONTRACT_TYPE_Cash loans','NAME_CONTRACT_TYPE_Consumer loans','NAME_CONTRACT_TYPE_Revolving loans','NAME_CONTRACT_TYPE_XNA','WEEKDAY_APPR_PROCESS_START_FRIDAY_y','WEEKDAY_APPR_PROCESS_START_MONDAY_y','WEEKDAY_APPR_PROCESS_START_SATURDAY_y','WEEKDAY_APPR_PROCESS_START_SUNDAY_y','WEEKDAY_APPR_PROCESS_START_THURSDAY_y','WEEKDAY_APPR_PROCESS_START_TUESDAY_y','WEEKDAY_APPR_PROCESS_START_WEDNESDAY_y','NAME_CASH_LOAN_PURPOSE_Building a house or an annex','NAME_CASH_LOAN_PURPOSE_Business development','NAME_CASH_LOAN_PURPOSE_Buying a garage','NAME_CASH_LOAN_PURPOSE_Buying a holiday home / land','NAME_CASH_LOAN_PURPOSE_Buying a home','NAME_CASH_LOAN_PURPOSE_Buying a new car','NAME_CASH_LOAN_PURPOSE_Buying a used car','NAME_CASH_LOAN_PURPOSE_Car repairs','NAME_CASH_LOAN_PURPOSE_Education','NAME_CASH_LOAN_PURPOSE_Everyday expenses','NAME_CASH_LOAN_PURPOSE_Furniture','NAME_CASH_LOAN_PURPOSE_Gasification / water supply','NAME_CASH_LOAN_PURPOSE_Hobby','NAME_CASH_LOAN_PURPOSE_Journey','NAME_CASH_LOAN_PURPOSE_Medicine','NAME_CASH_LOAN_PURPOSE_Money for a third person','NAME_CASH_LOAN_PURPOSE_Other','NAME_CASH_LOAN_PURPOSE_Payments on other loans','NAME_CASH_LOAN_PURPOSE_Purchase of electronic equipment','NAME_CASH_LOAN_PURPOSE_Refusal to name the goal','NAME_CASH_LOAN_PURPOSE_Repairs','NAME_CASH_LOAN_PURPOSE_Urgent needs','NAME_CASH_LOAN_PURPOSE_Wedding / gift / holiday','NAME_CASH_LOAN_PURPOSE_XAP','NAME_CASH_LOAN_PURPOSE_XNA','NAME_CONTRACT_STATUS_Approved','NAME_CONTRACT_STATUS_Canceled_y','NAME_CONTRACT_STATUS_Refused_y','NAME_CONTRACT_STATUS_Unused offer','NAME_PAYMENT_TYPE_Cash through the bank','NAME_PAYMENT_TYPE_Cashless from the account of the employer','NAME_PAYMENT_TYPE_Non-cash from your account','NAME_PAYMENT_TYPE_XNA','CODE_REJECT_REASON_CLIENT','CODE_REJECT_REASON_HC','CODE_REJECT_REASON_LIMIT','CODE_REJECT_REASON_SCO','CODE_REJECT_REASON_SCOFR','CODE_REJECT_REASON_SYSTEM','CODE_REJECT_REASON_VERIF','CODE_REJECT_REASON_XAP','CODE_REJECT_REASON_XNA','NAME_CLIENT_TYPE_New','NAME_CLIENT_TYPE_Refreshed','NAME_CLIENT_TYPE_Repeater','NAME_CLIENT_TYPE_XNA','NAME_GOODS_CATEGORY_Additional Service','NAME_GOODS_CATEGORY_Animals','NAME_GOODS_CATEGORY_Audio/Video','NAME_GOODS_CATEGORY_Auto Accessories','NAME_GOODS_CATEGORY_Clothing and Accessories','NAME_GOODS_CATEGORY_Computers','NAME_GOODS_CATEGORY_Construction Materials','NAME_GOODS_CATEGORY_Consumer Electronics','NAME_GOODS_CATEGORY_Direct Sales','NAME_GOODS_CATEGORY_Education','NAME_GOODS_CATEGORY_Fitness','NAME_GOODS_CATEGORY_Furniture','NAME_GOODS_CATEGORY_Gardening','NAME_GOODS_CATEGORY_Homewares','NAME_GOODS_CATEGORY_House Construction','NAME_GOODS_CATEGORY_Insurance','NAME_GOODS_CATEGORY_Jewelry','NAME_GOODS_CATEGORY_Medical Supplies','NAME_GOODS_CATEGORY_Medicine','NAME_GOODS_CATEGORY_Mobile','NAME_GOODS_CATEGORY_Office Appliances','NAME_GOODS_CATEGORY_Other','NAME_GOODS_CATEGORY_Photo / Cinema Equipment','NAME_GOODS_CATEGORY_Sport and Leisure','NAME_GOODS_CATEGORY_Tourism','NAME_GOODS_CATEGORY_Vehicles','NAME_GOODS_CATEGORY_Weapon','NAME_GOODS_CATEGORY_XNA','NAME_PORTFOLIO_Cards','NAME_PORTFOLIO_Cars','NAME_PORTFOLIO_Cash','NAME_PORTFOLIO_POS','NAME_PORTFOLIO_XNA','NAME_PRODUCT_TYPE_XNA','NAME_PRODUCT_TYPE_walk-in','NAME_PRODUCT_TYPE_x-sell','CHANNEL_TYPE_AP+ (Cash loan)','CHANNEL_TYPE_Car dealer','CHANNEL_TYPE_Channel of corporate sales','CHANNEL_TYPE_Contact center','CHANNEL_TYPE_Country-wide','CHANNEL_TYPE_Credit and cash offices','CHANNEL_TYPE_Regional / Local','CHANNEL_TYPE_Stone','NAME_SELLER_INDUSTRY_Auto technology','NAME_SELLER_INDUSTRY_Clothing','NAME_SELLER_INDUSTRY_Connectivity','NAME_SELLER_INDUSTRY_Construction','NAME_SELLER_INDUSTRY_Consumer electronics','NAME_SELLER_INDUSTRY_Furniture','NAME_SELLER_INDUSTRY_Industry','NAME_SELLER_INDUSTRY_Jewelry','NAME_SELLER_INDUSTRY_MLM partners','NAME_SELLER_INDUSTRY_Tourism','NAME_SELLER_INDUSTRY_XNA','NAME_YIELD_GROUP_XNA','NAME_YIELD_GROUP_high','NAME_YIELD_GROUP_low_action','NAME_YIELD_GROUP_low_normal','NAME_YIELD_GROUP_middle','PRODUCT_COMBINATION_Card Street','PRODUCT_COMBINATION_Card X-Sell','PRODUCT_COMBINATION_Cash','PRODUCT_COMBINATION_Cash Street: high','PRODUCT_COMBINATION_Cash Street: low','PRODUCT_COMBINATION_Cash Street: middle','PRODUCT_COMBINATION_Cash X-Sell: high','PRODUCT_COMBINATION_Cash X-Sell: low','PRODUCT_COMBINATION_Cash X-Sell: middle','PRODUCT_COMBINATION_POS household with interest','PRODUCT_COMBINATION_POS household without interest','PRODUCT_COMBINATION_POS industry with interest','PRODUCT_COMBINATION_POS industry without interest','PRODUCT_COMBINATION_POS mobile with interest','PRODUCT_COMBINATION_POS mobile without interest','PRODUCT_COMBINATION_POS other with interest','PRODUCT_COMBINATION_POS others without interest','TARGET','NAME_CONTRACT_TYPE_y','CODE_GENDER_y','FLAG_OWN_CAR_y','FLAG_OWN_REALTY_y','CNT_CHILDREN_y','AMT_INCOME_TOTAL_y','AMT_CREDIT_y','AMT_ANNUITY_y','AMT_GOODS_PRICE_y','NAME_TYPE_SUITE','NAME_INCOME_TYPE','NAME_EDUCATION_TYPE','NAME_FAMILY_STATUS','NAME_HOUSING_TYPE','REGION_POPULATION_RELATIVE_y','DAYS_BIRTH_y','DAYS_EMPLOYED_y','DAYS_REGISTRATION_y','DAYS_ID_PUBLISH_y','OWN_CAR_AGE','FLAG_MOBIL_y','FLAG_EMP_PHONE_y','FLAG_WORK_PHONE_y','FLAG_CONT_MOBILE_y','FLAG_PHONE_y','FLAG_EMAIL_y','OCCUPATION_TYPE','CNT_FAM_MEMBERS_y','REGION_RATING_CLIENT_y','REGION_RATING_CLIENT_W_CITY_y','WEEKDAY_APPR_PROCESS_START','HOUR_APPR_PROCESS_START_y','REG_REGION_NOT_LIVE_REGION_y','REG_REGION_NOT_WORK_REGION_y','LIVE_REGION_NOT_WORK_REGION_y','REG_CITY_NOT_LIVE_CITY_y','REG_CITY_NOT_WORK_CITY_y','LIVE_CITY_NOT_WORK_CITY_y','ORGANIZATION_TYPE','EXT_SOURCE_1','EXT_SOURCE_2_y','EXT_SOURCE_3_y','APARTMENTS_AVG','BASEMENTAREA_AVG','YEARS_BEGINEXPLUATATION_AVG','YEARS_BUILD_AVG','COMMONAREA_AVG','ELEVATORS_AVG','ENTRANCES_AVG','FLOORSMAX_AVG','FLOORSMIN_AVG','LANDAREA_AVG','LIVINGAPARTMENTS_AVG','LIVINGAREA_AVG','NONLIVINGAPARTMENTS_AVG','NONLIVINGAREA_AVG','APARTMENTS_MODE','BASEMENTAREA_MODE','YEARS_BEGINEXPLUATATION_MODE','YEARS_BUILD_MODE','COMMONAREA_MODE','ELEVATORS_MODE','ENTRANCES_MODE','FLOORSMAX_MODE','FLOORSMIN_MODE','LANDAREA_MODE','LIVINGAPARTMENTS_MODE','LIVINGAREA_MODE','NONLIVINGAPARTMENTS_MODE','NONLIVINGAREA_MODE','APARTMENTS_MEDI','BASEMENTAREA_MEDI','YEARS_BEGINEXPLUATATION_MEDI','YEARS_BUILD_MEDI','COMMONAREA_MEDI','ELEVATORS_MEDI','ENTRANCES_MEDI','FLOORSMAX_MEDI','FLOORSMIN_MEDI','LANDAREA_MEDI','LIVINGAPARTMENTS_MEDI','LIVINGAREA_MEDI','NONLIVINGAPARTMENTS_MEDI','NONLIVINGAREA_MEDI','FONDKAPREMONT_MODE','HOUSETYPE_MODE','TOTALAREA_MODE','WALLSMATERIAL_MODE','EMERGENCYSTATE_MODE','OBS_30_CNT_SOCIAL_CIRCLE_y','DEF_30_CNT_SOCIAL_CIRCLE_y','OBS_60_CNT_SOCIAL_CIRCLE_y','DEF_60_CNT_SOCIAL_CIRCLE_y','DAYS_LAST_PHONE_CHANGE_y','FLAG_DOCUMENT_2_y','FLAG_DOCUMENT_3_y','FLAG_DOCUMENT_4_y','FLAG_DOCUMENT_5_y','FLAG_DOCUMENT_6_y','FLAG_DOCUMENT_7_y','FLAG_DOCUMENT_8_y','FLAG_DOCUMENT_9_y','FLAG_DOCUMENT_10_y','FLAG_DOCUMENT_11_y','FLAG_DOCUMENT_12_y','FLAG_DOCUMENT_13_y','FLAG_DOCUMENT_14_y','FLAG_DOCUMENT_15_y','FLAG_DOCUMENT_16_y','FLAG_DOCUMENT_17_y','FLAG_DOCUMENT_18_y','FLAG_DOCUMENT_19_y','FLAG_DOCUMENT_20_y','FLAG_DOCUMENT_21_y','AMT_REQ_CREDIT_BUREAU_HOUR_y','AMT_REQ_CREDIT_BUREAU_DAY_y','AMT_REQ_CREDIT_BUREAU_WEEK_y','AMT_REQ_CREDIT_BUREAU_MON_y','AMT_REQ_CREDIT_BUREAU_QRT_y','AMT_REQ_CREDIT_BUREAU_YEAR_y'
]
#目前測試集欄位
atestcl=['SK_ID_CURR','DAYS_REGISTRATION','HOUR_APPR_PROCESS_START','AMT_REQ_CREDIT_BUREAU_WEEK','DEF_60_CNT_SOCIAL_CIRCLE','EXT_SOURCE_3','CNT_CHILDREN','AMT_REQ_CREDIT_BUREAU_QRT','CNT_FAM_MEMBERS','DAYS_ID_PUBLISH','DEF_30_CNT_SOCIAL_CIRCLE','REGION_POPULATION_RELATIVE','EXT_SOURCE_2','AMT_REQ_CREDIT_BUREAU_YEAR','AMT_ANNUITY','OBS_30_CNT_SOCIAL_CIRCLE','DAYS_LAST_PHONE_CHANGE','AMT_CREDIT','DAYS_EMPLOYED','REGION_RATING_CLIENT_W_CITY','AMT_REQ_CREDIT_BUREAU_DAY','AMT_GOODS_PRICE','AMT_REQ_CREDIT_BUREAU_HOUR','OBS_60_CNT_SOCIAL_CIRCLE','REGION_RATING_CLIENT','DAYS_BIRTH','AMT_REQ_CREDIT_BUREAU_MON','AMT_INCOME_TOTAL','DAYS_CREDIT_mean','CREDIT_DAY_OVERDUE_mean','DAYS_CREDIT_ENDDATE_mean','DAYS_ENDDATE_FACT_mean','CNT_CREDIT_PROLONG_mean','AMT_CREDIT_SUM_mean','AMT_CREDIT_SUM_DEBT_mean','AMT_CREDIT_SUM_LIMIT_mean','AMT_CREDIT_SUM_OVERDUE_mean','DAYS_CREDIT_UPDATE_mean','MONTHS_BALANCE_mean_x','CNT_INSTALMENT_mean','CNT_INSTALMENT_FUTURE_mean','SK_DPD_mean_x','SK_DPD_DEF_mean_x','NUM_INSTALMENT_VERSION_mean','NUM_INSTALMENT_NUMBER_mean','DAYS_INSTALMENT_mean','DAYS_ENTRY_PAYMENT_mean','AMT_INSTALMENT_mean','AMT_PAYMENT_mean','MONTHS_BALANCE_mean_y','AMT_BALANCE_mean','AMT_CREDIT_LIMIT_ACTUAL_mean','AMT_DRAWINGS_ATM_CURRENT_mean','AMT_DRAWINGS_CURRENT_mean','AMT_DRAWINGS_OTHER_CURRENT_mean','AMT_DRAWINGS_POS_CURRENT_mean','AMT_INST_MIN_REGULARITY_mean','AMT_PAYMENT_CURRENT_mean','AMT_PAYMENT_TOTAL_CURRENT_mean','AMT_RECEIVABLE_PRINCIPAL_mean','AMT_RECIVABLE_mean','AMT_TOTAL_RECEIVABLE_mean','CNT_DRAWINGS_ATM_CURRENT_mean','CNT_DRAWINGS_CURRENT_mean','CNT_DRAWINGS_OTHER_CURRENT_mean','CNT_DRAWINGS_POS_CURRENT_mean','CNT_INSTALMENT_MATURE_CUM_mean','SK_DPD_mean_y','SK_DPD_DEF_mean_y','AMT_ANNUITY_mean','AMT_APPLICATION_mean','AMT_CREDIT_mean','AMT_GOODS_PRICE_mean','HOUR_APPR_PROCESS_START_mean','DAYS_DECISION_mean','SELLERPLACE_AREA_mean','CNT_PAYMENT_mean','DAYS_FIRST_DRAWING_mean','DAYS_FIRST_DUE_mean','DAYS_LAST_DUE_1ST_VERSION_mean','DAYS_LAST_DUE_mean','DAYS_TERMINATION_mean','NAME_CONTRACT_TYPE','CODE_GENDER','FLAG_OWN_CAR','FLAG_OWN_REALTY','FLAG_MOBIL','FLAG_EMP_PHONE','FLAG_WORK_PHONE','FLAG_CONT_MOBILE','FLAG_PHONE','FLAG_EMAIL','REG_REGION_NOT_LIVE_REGION','REG_REGION_NOT_WORK_REGION','LIVE_REGION_NOT_WORK_REGION','REG_CITY_NOT_LIVE_CITY','REG_CITY_NOT_WORK_CITY','LIVE_CITY_NOT_WORK_CITY','FLAG_DOCUMENT_2','FLAG_DOCUMENT_3','FLAG_DOCUMENT_4','FLAG_DOCUMENT_5','FLAG_DOCUMENT_6','FLAG_DOCUMENT_7','FLAG_DOCUMENT_8','FLAG_DOCUMENT_9','FLAG_DOCUMENT_10','FLAG_DOCUMENT_11','FLAG_DOCUMENT_12','FLAG_DOCUMENT_13','FLAG_DOCUMENT_14','FLAG_DOCUMENT_15','FLAG_DOCUMENT_16','FLAG_DOCUMENT_17','FLAG_DOCUMENT_18','FLAG_DOCUMENT_19','FLAG_DOCUMENT_20','FLAG_DOCUMENT_21','FLAG_LAST_APPL_PER_CONTRACT','NFLAG_LAST_APPL_IN_DAY','NFLAG_INSURED_ON_APPROVAL','NAME_TYPE_SUITE_Children','NAME_TYPE_SUITE_Family','NAME_TYPE_SUITE_Group of people','NAME_TYPE_SUITE_Other_A','NAME_TYPE_SUITE_Other_B','NAME_TYPE_SUITE_Spouse, partner','NAME_TYPE_SUITE_Unaccompanied','NAME_INCOME_TYPE_Businessman','NAME_INCOME_TYPE_Commercial associate','NAME_INCOME_TYPE_Pensioner','NAME_INCOME_TYPE_State servant','NAME_INCOME_TYPE_Student','NAME_INCOME_TYPE_Unemployed','NAME_INCOME_TYPE_Working','NAME_EDUCATION_TYPE_Academic degree','NAME_EDUCATION_TYPE_Higher education','NAME_EDUCATION_TYPE_Incomplete higher','NAME_EDUCATION_TYPE_Lower secondary','NAME_EDUCATION_TYPE_Secondary / secondary special','NAME_FAMILY_STATUS_Civil marriage','NAME_FAMILY_STATUS_Married','NAME_FAMILY_STATUS_Separated','NAME_FAMILY_STATUS_Single / not married','NAME_FAMILY_STATUS_Widow','NAME_HOUSING_TYPE_Co-op apartment','NAME_HOUSING_TYPE_House / apartment','NAME_HOUSING_TYPE_Municipal apartment','NAME_HOUSING_TYPE_Office apartment','NAME_HOUSING_TYPE_Rented apartment','NAME_HOUSING_TYPE_With parents','OCCUPATION_TYPE_Accountants','OCCUPATION_TYPE_Cleaning staff','OCCUPATION_TYPE_Cooking staff','OCCUPATION_TYPE_Core staff','OCCUPATION_TYPE_Drivers','OCCUPATION_TYPE_HR staff','OCCUPATION_TYPE_High skill tech staff','OCCUPATION_TYPE_IT staff','OCCUPATION_TYPE_Laborers','OCCUPATION_TYPE_Low-skill Laborers','OCCUPATION_TYPE_Managers','OCCUPATION_TYPE_Medicine staff','OCCUPATION_TYPE_Private service staff','OCCUPATION_TYPE_Realty agents','OCCUPATION_TYPE_Sales staff','OCCUPATION_TYPE_Secretaries','OCCUPATION_TYPE_Security staff','OCCUPATION_TYPE_Waiters/barmen staff','WEEKDAY_APPR_PROCESS_START_FRIDAY_x','WEEKDAY_APPR_PROCESS_START_MONDAY_x','WEEKDAY_APPR_PROCESS_START_SATURDAY_x','WEEKDAY_APPR_PROCESS_START_SUNDAY_x','WEEKDAY_APPR_PROCESS_START_THURSDAY_x','WEEKDAY_APPR_PROCESS_START_TUESDAY_x','WEEKDAY_APPR_PROCESS_START_WEDNESDAY_x','ORGANIZATION_TYPE_Advertising','ORGANIZATION_TYPE_Agriculture','ORGANIZATION_TYPE_Bank','ORGANIZATION_TYPE_Business Entity Type 1','ORGANIZATION_TYPE_Business Entity Type 2','ORGANIZATION_TYPE_Business Entity Type 3','ORGANIZATION_TYPE_Cleaning','ORGANIZATION_TYPE_Construction','ORGANIZATION_TYPE_Culture','ORGANIZATION_TYPE_Electricity','ORGANIZATION_TYPE_Emergency','ORGANIZATION_TYPE_Government','ORGANIZATION_TYPE_Hotel','ORGANIZATION_TYPE_Housing','ORGANIZATION_TYPE_Industry: type 1','ORGANIZATION_TYPE_Industry: type 10','ORGANIZATION_TYPE_Industry: type 11','ORGANIZATION_TYPE_Industry: type 12','ORGANIZATION_TYPE_Industry: type 13','ORGANIZATION_TYPE_Industry: type 2','ORGANIZATION_TYPE_Industry: type 3','ORGANIZATION_TYPE_Industry: type 4','ORGANIZATION_TYPE_Industry: type 5','ORGANIZATION_TYPE_Industry: type 6','ORGANIZATION_TYPE_Industry: type 7','ORGANIZATION_TYPE_Industry: type 8','ORGANIZATION_TYPE_Industry: type 9','ORGANIZATION_TYPE_Insurance','ORGANIZATION_TYPE_Kindergarten','ORGANIZATION_TYPE_Legal Services','ORGANIZATION_TYPE_Medicine','ORGANIZATION_TYPE_Military','ORGANIZATION_TYPE_Mobile','ORGANIZATION_TYPE_Other','ORGANIZATION_TYPE_Police','ORGANIZATION_TYPE_Postal','ORGANIZATION_TYPE_Realtor','ORGANIZATION_TYPE_Religion','ORGANIZATION_TYPE_Restaurant','ORGANIZATION_TYPE_School','ORGANIZATION_TYPE_Security','ORGANIZATION_TYPE_Security Ministries','ORGANIZATION_TYPE_Self-employed','ORGANIZATION_TYPE_Services','ORGANIZATION_TYPE_Telecom','ORGANIZATION_TYPE_Trade: type 1','ORGANIZATION_TYPE_Trade: type 2','ORGANIZATION_TYPE_Trade: type 3','ORGANIZATION_TYPE_Trade: type 4','ORGANIZATION_TYPE_Trade: type 5','ORGANIZATION_TYPE_Trade: type 6','ORGANIZATION_TYPE_Trade: type 7','ORGANIZATION_TYPE_Transport: type 1','ORGANIZATION_TYPE_Transport: type 2','ORGANIZATION_TYPE_Transport: type 3','ORGANIZATION_TYPE_Transport: type 4','ORGANIZATION_TYPE_University','ORGANIZATION_TYPE_XNA','SK_ID_BUREAU','CREDIT_ACTIVE_Active','CREDIT_ACTIVE_Bad debt','CREDIT_ACTIVE_Closed','CREDIT_ACTIVE_Sold','CREDIT_CURRENCY_currency 1','CREDIT_CURRENCY_currency 2','CREDIT_CURRENCY_currency 3','CREDIT_CURRENCY_currency 4','CREDIT_TYPE_Another type of loan','CREDIT_TYPE_Car loan','CREDIT_TYPE_Cash loan (non-earmarked)','CREDIT_TYPE_Consumer credit','CREDIT_TYPE_Credit card','CREDIT_TYPE_Interbank credit','CREDIT_TYPE_Loan for business development','CREDIT_TYPE_Loan for purchase of shares (margin lending)','CREDIT_TYPE_Loan for the purchase of equipment','CREDIT_TYPE_Loan for working capital replenishment','CREDIT_TYPE_Microloan','CREDIT_TYPE_Mobile operator loan','CREDIT_TYPE_Mortgage','CREDIT_TYPE_Real estate loan','CREDIT_TYPE_Unknown type of loan','STATUS_0','STATUS_1','STATUS_2','STATUS_3','STATUS_4','STATUS_5','STATUS_C','STATUS_X','NAME_CONTRACT_STATUS_Active_x','NAME_CONTRACT_STATUS_Amortized debt','NAME_CONTRACT_STATUS_Approved_x','NAME_CONTRACT_STATUS_Canceled_x','NAME_CONTRACT_STATUS_Completed_x','NAME_CONTRACT_STATUS_Demand_x','NAME_CONTRACT_STATUS_Returned to the store','NAME_CONTRACT_STATUS_Signed_x','NAME_CONTRACT_STATUS_XNA','NAME_CONTRACT_STATUS_Active_y','NAME_CONTRACT_STATUS_Approved_y','NAME_CONTRACT_STATUS_Completed_y','NAME_CONTRACT_STATUS_Demand_y','NAME_CONTRACT_STATUS_Refused_x','NAME_CONTRACT_STATUS_Sent proposal','NAME_CONTRACT_STATUS_Signed_y','SK_ID_PREV','NAME_CONTRACT_TYPE_Cash loans','NAME_CONTRACT_TYPE_Consumer loans','NAME_CONTRACT_TYPE_Revolving loans','NAME_CONTRACT_TYPE_XNA','WEEKDAY_APPR_PROCESS_START_FRIDAY_y','WEEKDAY_APPR_PROCESS_START_MONDAY_y','WEEKDAY_APPR_PROCESS_START_SATURDAY_y','WEEKDAY_APPR_PROCESS_START_SUNDAY_y','WEEKDAY_APPR_PROCESS_START_THURSDAY_y','WEEKDAY_APPR_PROCESS_START_TUESDAY_y','WEEKDAY_APPR_PROCESS_START_WEDNESDAY_y','NAME_CASH_LOAN_PURPOSE_Building a house or an annex','NAME_CASH_LOAN_PURPOSE_Business development','NAME_CASH_LOAN_PURPOSE_Buying a garage','NAME_CASH_LOAN_PURPOSE_Buying a holiday home / land','NAME_CASH_LOAN_PURPOSE_Buying a home','NAME_CASH_LOAN_PURPOSE_Buying a new car','NAME_CASH_LOAN_PURPOSE_Buying a used car','NAME_CASH_LOAN_PURPOSE_Car repairs','NAME_CASH_LOAN_PURPOSE_Education','NAME_CASH_LOAN_PURPOSE_Everyday expenses','NAME_CASH_LOAN_PURPOSE_Furniture','NAME_CASH_LOAN_PURPOSE_Gasification / water supply','NAME_CASH_LOAN_PURPOSE_Hobby','NAME_CASH_LOAN_PURPOSE_Journey','NAME_CASH_LOAN_PURPOSE_Medicine','NAME_CASH_LOAN_PURPOSE_Money for a third person','NAME_CASH_LOAN_PURPOSE_Other','NAME_CASH_LOAN_PURPOSE_Payments on other loans','NAME_CASH_LOAN_PURPOSE_Purchase of electronic equipment','NAME_CASH_LOAN_PURPOSE_Refusal to name the goal','NAME_CASH_LOAN_PURPOSE_Repairs','NAME_CASH_LOAN_PURPOSE_Urgent needs','NAME_CASH_LOAN_PURPOSE_Wedding / gift / holiday','NAME_CASH_LOAN_PURPOSE_XAP','NAME_CASH_LOAN_PURPOSE_XNA','NAME_CONTRACT_STATUS_Approved','NAME_CONTRACT_STATUS_Canceled_y','NAME_CONTRACT_STATUS_Refused_y','NAME_CONTRACT_STATUS_Unused offer','NAME_PAYMENT_TYPE_Cash through the bank','NAME_PAYMENT_TYPE_Cashless from the account of the employer','NAME_PAYMENT_TYPE_Non-cash from your account','NAME_PAYMENT_TYPE_XNA','CODE_REJECT_REASON_CLIENT','CODE_REJECT_REASON_HC','CODE_REJECT_REASON_LIMIT','CODE_REJECT_REASON_SCO','CODE_REJECT_REASON_SCOFR','CODE_REJECT_REASON_SYSTEM','CODE_REJECT_REASON_VERIF','CODE_REJECT_REASON_XAP','CODE_REJECT_REASON_XNA','NAME_CLIENT_TYPE_New','NAME_CLIENT_TYPE_Refreshed','NAME_CLIENT_TYPE_Repeater','NAME_CLIENT_TYPE_XNA','NAME_GOODS_CATEGORY_Additional Service','NAME_GOODS_CATEGORY_Animals','NAME_GOODS_CATEGORY_Audio/Video','NAME_GOODS_CATEGORY_Auto Accessories','NAME_GOODS_CATEGORY_Clothing and Accessories','NAME_GOODS_CATEGORY_Computers','NAME_GOODS_CATEGORY_Construction Materials','NAME_GOODS_CATEGORY_Consumer Electronics','NAME_GOODS_CATEGORY_Direct Sales','NAME_GOODS_CATEGORY_Education','NAME_GOODS_CATEGORY_Fitness','NAME_GOODS_CATEGORY_Furniture','NAME_GOODS_CATEGORY_Gardening','NAME_GOODS_CATEGORY_Homewares','NAME_GOODS_CATEGORY_House Construction','NAME_GOODS_CATEGORY_Insurance','NAME_GOODS_CATEGORY_Jewelry','NAME_GOODS_CATEGORY_Medical Supplies','NAME_GOODS_CATEGORY_Medicine','NAME_GOODS_CATEGORY_Mobile','NAME_GOODS_CATEGORY_Office Appliances','NAME_GOODS_CATEGORY_Other','NAME_GOODS_CATEGORY_Photo / Cinema Equipment','NAME_GOODS_CATEGORY_Sport and Leisure','NAME_GOODS_CATEGORY_Tourism','NAME_GOODS_CATEGORY_Vehicles','NAME_GOODS_CATEGORY_Weapon','NAME_GOODS_CATEGORY_XNA','NAME_PORTFOLIO_Cards','NAME_PORTFOLIO_Cars','NAME_PORTFOLIO_Cash','NAME_PORTFOLIO_POS','NAME_PORTFOLIO_XNA','NAME_PRODUCT_TYPE_XNA','NAME_PRODUCT_TYPE_walk-in','NAME_PRODUCT_TYPE_x-sell','CHANNEL_TYPE_AP+ (Cash loan)','CHANNEL_TYPE_Car dealer','CHANNEL_TYPE_Channel of corporate sales','CHANNEL_TYPE_Contact center','CHANNEL_TYPE_Country-wide','CHANNEL_TYPE_Credit and cash offices','CHANNEL_TYPE_Regional / Local','CHANNEL_TYPE_Stone','NAME_SELLER_INDUSTRY_Auto technology','NAME_SELLER_INDUSTRY_Clothing','NAME_SELLER_INDUSTRY_Connectivity','NAME_SELLER_INDUSTRY_Construction','NAME_SELLER_INDUSTRY_Consumer electronics','NAME_SELLER_INDUSTRY_Furniture','NAME_SELLER_INDUSTRY_Industry','NAME_SELLER_INDUSTRY_Jewelry','NAME_SELLER_INDUSTRY_MLM partners','NAME_SELLER_INDUSTRY_Tourism','NAME_SELLER_INDUSTRY_XNA','NAME_YIELD_GROUP_XNA','NAME_YIELD_GROUP_high','NAME_YIELD_GROUP_low_action','NAME_YIELD_GROUP_low_normal','NAME_YIELD_GROUP_middle','PRODUCT_COMBINATION_Card Street','PRODUCT_COMBINATION_Card X-Sell','PRODUCT_COMBINATION_Cash','PRODUCT_COMBINATION_Cash Street: high','PRODUCT_COMBINATION_Cash Street: low','PRODUCT_COMBINATION_Cash Street: middle','PRODUCT_COMBINATION_Cash X-Sell: high','PRODUCT_COMBINATION_Cash X-Sell: low','PRODUCT_COMBINATION_Cash X-Sell: middle','PRODUCT_COMBINATION_POS household with interest','PRODUCT_COMBINATION_POS household without interest','PRODUCT_COMBINATION_POS industry with interest','PRODUCT_COMBINATION_POS industry without interest','PRODUCT_COMBINATION_POS mobile with interest','PRODUCT_COMBINATION_POS mobile without interest','PRODUCT_COMBINATION_POS other with interest','PRODUCT_COMBINATION_POS others without interest']
#目前訓練集與訓練集欄位的交集
unicl=set(atcl)&set(atestcl)
#訓練集 保留與測試集欄位的交集
at_merge=at_merge[unicl]#(655,354)
at_targe=at.loc[:,['SK_ID_CURR','TARGET']]#訓練集 的'SK_ID_CURR','TARGET'欄位
at_merge=at_merge.merge(at_targe, on='SK_ID_CURR',how='left')#(655,355)#將 'TARGET'欄位加入訓練集
at_merge.to_csv('at_merge.csv')#匯出.csv
#測試集 保留與訓練集欄位的交集
atest_merge=atest_merge[unicl]##(7621,354)
atest_merge.to_csv('atest_merge.csv')#匯出.csv

############################################################################################
#                      TODO
#############################################################################################
#刪除有遺漏值的列後資料太少 擬定較嚴謹的刪除列原則
#***欄位合併時會有欄位減少的情況!!!