# -*- coding: utf-8 -*-
"""
Created on Fri Jun  4 15:25:44 2021

@author: marks
"""

############################################################################################
#                      匯入資料 
############################################################################################
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
#                      刪除離群值 
############################################################################################
at['DAYS_EMPLOYED'].replace(365243, np.nan, inplace = True)#申請人申請時就業的天數離群值
at.loc[at['AMT_INCOME_TOTAL'] > 1e8, 'AMT_INCOME_TOTAL'] = np.nan#申請人收入
at.loc[at['AMT_REQ_CREDIT_BUREAU_QRT'] > 10, 'AMT_REQ_CREDIT_BUREAU_QRT'] = np.nan#客戶申請前三個月向信用機構查詢客戶信息次數
at.loc[at['OBS_30_CNT_SOCIAL_CIRCLE'] > 40, 'OBS_30_CNT_SOCIAL_CIRCLE'] = np.nan#客戶逾期30天的次數 47、348

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

############計算bureau.csv 中數值欄位的平均值與加總
b_num_aggregations = {'DAYS_CREDIT':['mean'],'CREDIT_DAY_OVERDUE':['mean'],'DAYS_CREDIT_ENDDATE':['mean'],'DAYS_ENDDATE_FACT':['mean'],'CNT_CREDIT_PROLONG':['mean'],'AMT_CREDIT_SUM':['mean'],'AMT_CREDIT_SUM_DEBT':['mean'],'AMT_CREDIT_SUM_LIMIT':['mean'],'AMT_CREDIT_SUM_OVERDUE':['mean'],'DAYS_CREDIT_UPDATE':['mean']}
b_num_agg=b_num.groupby('SK_ID_CURR',as_index=False).agg(b_num_aggregations)# 將bureau.csv 中的數值欄位 以Key'SK_ID_BUREAU'分群 並計算平均數
############讀取POS_CASH_balance.csv 中數值欄位 
PC_num=PC[['SK_ID_PREV','SK_ID_CURR','MONTHS_BALANCE','CNT_INSTALMENT','CNT_INSTALMENT_FUTURE','SK_DPD','SK_DPD_DEF']]# 讀取POS_CASH_balance.csv 中的數值欄位'MONTHS_BALANCE' 並保留Key'SK_ID_PREV' 'SK_ID_CURR'
############計算POS_CASH_balance.csv 中數值欄位的平均值與加總
pcb_num_aggregations = {'MONTHS_BALANCE':['mean'],'CNT_INSTALMENT':['mean'],'CNT_INSTALMENT_FUTURE':['mean'],'SK_DPD':['mean'],'SK_DPD_DEF':['mean']}
PC_num_agg=PC_num.groupby('SK_ID_CURR').agg(pcb_num_aggregations)# 將POS_CASH_balance.csv 中的數值欄位 以Key'SK_ID_PREV'分群 並計算平均數
PC_num_agg.reset_index(level='SK_ID_CURR', col_level=1)
############讀取installments_payments.csv 中數值欄位 
i_num=i[['SK_ID_PREV','SK_ID_CURR','NUM_INSTALMENT_VERSION','NUM_INSTALMENT_NUMBER','DAYS_INSTALMENT','DAYS_ENTRY_PAYMENT','AMT_INSTALMENT','AMT_PAYMENT']]# 讀取installments_payments.csv 中的數值欄位 並保留Key SK_ID_PREV' 'SK_ID_CURR'
############計算installments_payments.csv 中數值欄位的平均值與加總
i_num_aggregations = {'NUM_INSTALMENT_VERSION':['mean'],'NUM_INSTALMENT_NUMBER':['mean'],'DAYS_INSTALMENT':['mean'],'DAYS_ENTRY_PAYMENT':['mean'],'AMT_INSTALMENT':['mean'],'AMT_PAYMENT':['mean']}
i_num_agg=i_num.groupby('SK_ID_CURR').agg(i_num_aggregations)# 將installments_payments.csv 中的數值欄位 以Key'SK_ID_PREV'分群 並計算平均數
i_num_agg.reset_index(level='SK_ID_CURR', col_level=1)
############讀取credit_card_balance.csv 中數值欄位 
cc_num=cc[['SK_ID_PREV','SK_ID_CURR','MONTHS_BALANCE','AMT_BALANCE','AMT_CREDIT_LIMIT_ACTUAL','AMT_DRAWINGS_ATM_CURRENT','AMT_DRAWINGS_CURRENT','AMT_DRAWINGS_OTHER_CURRENT','AMT_DRAWINGS_POS_CURRENT','AMT_INST_MIN_REGULARITY','AMT_PAYMENT_CURRENT','AMT_PAYMENT_TOTAL_CURRENT','AMT_RECEIVABLE_PRINCIPAL','AMT_RECIVABLE','AMT_TOTAL_RECEIVABLE','CNT_DRAWINGS_ATM_CURRENT','CNT_DRAWINGS_CURRENT','CNT_DRAWINGS_OTHER_CURRENT','CNT_DRAWINGS_POS_CURRENT','CNT_INSTALMENT_MATURE_CUM','SK_DPD','SK_DPD_DEF']]# 讀取credit_card_balance.csv 中的數值欄位 並保留Key'SK_ID_PREV' 'SK_ID_CURR'
############計算credit_card_balance.csv 中數值欄位的平均值與加總
cc_num_aggregations = {'MONTHS_BALANCE': ['mean'],'AMT_BALANCE': ['mean'],'AMT_CREDIT_LIMIT_ACTUAL': ['mean'],'AMT_DRAWINGS_ATM_CURRENT': ['mean'],'AMT_DRAWINGS_CURRENT': ['mean'],'AMT_DRAWINGS_OTHER_CURRENT': ['mean'],'AMT_DRAWINGS_POS_CURRENT': ['mean'],'AMT_INST_MIN_REGULARITY': ['mean'],'AMT_PAYMENT_CURRENT': ['mean'],'AMT_PAYMENT_TOTAL_CURRENT': ['mean'],'AMT_RECEIVABLE_PRINCIPAL': ['mean'],'AMT_RECIVABLE': ['mean'],'AMT_TOTAL_RECEIVABLE': ['mean'],'CNT_DRAWINGS_ATM_CURRENT': ['mean'],'CNT_DRAWINGS_CURRENT': ['mean'],'CNT_DRAWINGS_OTHER_CURRENT': ['mean'],'CNT_DRAWINGS_POS_CURRENT': ['mean'],'CNT_INSTALMENT_MATURE_CUM': ['mean'],'SK_DPD': ['mean'],'SK_DPD_DEF': ['mean']}
cc_num_agg=cc_num.groupby('SK_ID_CURR').agg(cc_num_aggregations)# 將credit_card_balance.csv 中的數值欄位 以Key'SK_ID_PREV'分群 並計算平均數
cc_num_agg.reset_index(level='SK_ID_CURR', col_level=1)
############分別讀取previous_application.csv 中 Boolean欄位 數值欄位 與 類別欄位(會轉成虛擬變數欄位)
p_num=previous_application[['SK_ID_PREV','SK_ID_CURR','AMT_ANNUITY','AMT_APPLICATION','AMT_CREDIT','AMT_GOODS_PRICE','HOUR_APPR_PROCESS_START','DAYS_DECISION','SELLERPLACE_AREA','CNT_PAYMENT','DAYS_FIRST_DRAWING','DAYS_FIRST_DUE','DAYS_LAST_DUE_1ST_VERSION','DAYS_LAST_DUE','DAYS_TERMINATION']]# 讀取previous_application.csv 中的數值欄位 並保留Key'SK_ID_PREV' 'SK_ID_CURR'()
############計算previous_application.csv 中數值欄位的平均值與加總
p_num_aggregations={'AMT_ANNUITY':['mean'],'AMT_APPLICATION':['mean'],'AMT_CREDIT':['mean'],'AMT_GOODS_PRICE':['mean'],'HOUR_APPR_PROCESS_START':['mean'],'DAYS_DECISION':['mean'],'SELLERPLACE_AREA':['mean'],'CNT_PAYMENT':['mean'],'DAYS_FIRST_DRAWING':['mean'],'DAYS_FIRST_DUE':['mean'],'DAYS_LAST_DUE_1ST_VERSION':['mean'],'DAYS_LAST_DUE':['mean'],'DAYS_TERMINATION':['mean']}
p_num_agg=p_num.groupby('SK_ID_CURR').agg(p_num_aggregations)# 將previous_application.csv 中的數值欄位 以Key'SK_ID_previous_application'分群 並計算平均數
p_num_agg.reset_index(level='SK_ID_CURR', col_level=1)
############讀取application_train.csv 中 數值欄位              
at_num=at[['SK_ID_CURR','DAYS_REGISTRATION','HOUR_APPR_PROCESS_START','AMT_REQ_CREDIT_BUREAU_WEEK','DEF_60_CNT_SOCIAL_CIRCLE','EXT_SOURCE_3','CNT_CHILDREN','AMT_REQ_CREDIT_BUREAU_QRT','CNT_FAM_MEMBERS','DAYS_ID_PUBLISH','DEF_30_CNT_SOCIAL_CIRCLE','REGION_POPULATION_RELATIVE','EXT_SOURCE_2','AMT_REQ_CREDIT_BUREAU_YEAR','AMT_ANNUITY','OBS_30_CNT_SOCIAL_CIRCLE','DAYS_LAST_PHONE_CHANGE','AMT_CREDIT','DAYS_EMPLOYED','REGION_RATING_CLIENT_W_CITY','AMT_REQ_CREDIT_BUREAU_DAY','AMT_GOODS_PRICE','AMT_REQ_CREDIT_BUREAU_HOUR','OBS_60_CNT_SOCIAL_CIRCLE','REGION_RATING_CLIENT','DAYS_BIRTH','AMT_REQ_CREDIT_BUREAU_MON','AMT_INCOME_TOTAL']]# 讀取application_train.csv 中的數值欄位 並保留Key 'SK_ID_CURR' (307511,28)
#&&&&&&&&&&&將application_train.csv 中 數值欄位與bureau_num_agg中欄位 合併
at_num=at_num.merge(b_num_agg,on='SK_ID_CURR',how='left')# 將bureau.csv的數值欄位與bureau_num_mean合併(307511,38)
#&&&&&&&&&&&將application_train.csv 中 數值欄位與POS_CASH_balance_num_agg中欄位 合併
at_num=at.merge(PC_num_agg,on='SK_ID_CURR',how='left')# 將bureau.csv的數值欄位與POS_CASH_balance_num合併(307511,78)
#&&&&&&&&&&&將at.csv 中 數值欄位與installments_payments_num_agg中欄位 合併
at_num=at.merge(i_num_agg,on='SK_ID_CURR',how='left')# 將bureau.csv的數值欄位與installments_payments_mean合併(307511,79)
#&&&&&&&&&&&將at.csv 中 數值欄位與credit_card_balance_num_agg中欄位 合併
at_num=at.merge(cc_num_agg,  on='SK_ID_CURR',how='left')# 將bureau.csv的數值欄位與credit_card_balance_mean合併(307511,93)
#&&&&&&&&&&&將at.csv 中 數值欄位與previous_application_num_agg中欄位 合併
at_num=at.merge(p_num_agg, on='SK_ID_CURR',how='left')# 將bureau.csv的數值欄位與previous_application_num_mean合併(307511,86)

atest_num=atest[['SK_ID_CURR','DAYS_REGISTRATION','HOUR_APPR_PROCESS_START','AMT_REQ_CREDIT_BUREAU_WEEK','DEF_60_CNT_SOCIAL_CIRCLE','EXT_SOURCE_3','CNT_CHILDREN','AMT_REQ_CREDIT_BUREAU_QRT','CNT_FAM_MEMBERS','DAYS_ID_PUBLISH','DEF_30_CNT_SOCIAL_CIRCLE','REGION_POPULATION_RELATIVE','EXT_SOURCE_2','AMT_REQ_CREDIT_BUREAU_YEAR','AMT_ANNUITY','OBS_30_CNT_SOCIAL_CIRCLE','DAYS_LAST_PHONE_CHANGE','AMT_CREDIT','DAYS_EMPLOYED','REGION_RATING_CLIENT_W_CITY','AMT_REQ_CREDIT_BUREAU_DAY','AMT_GOODS_PRICE','AMT_REQ_CREDIT_BUREAU_HOUR','OBS_60_CNT_SOCIAL_CIRCLE','REGION_RATING_CLIENT','DAYS_BIRTH','AMT_REQ_CREDIT_BUREAU_MON','AMT_INCOME_TOTAL']]# # 讀取application_test.csv 中的數值欄位 並保留Key 'SK_ID_CURR' (48744,28)

#&&&&&&&&&&&將application_train.csv 中 數值欄位與bureau_num_agg中欄位 合併
atest_num=atest_num.merge(b_num_agg,on='SK_ID_CURR',how='left')# 將bureau.csv的數值欄位與bureau_num_mean合併(307511,38)
#&&&&&&&&&&&將application_train.csv 中 數值欄位與POS_CASH_balance_num_agg中欄位 合併
atest_num=atest_num.merge(PC_num_agg,on='SK_ID_CURR',how='left')# 將bureau.csv的數值欄位與POS_CASH_balance_num合併(307511,43)
#&&&&&&&&&&&將atest_num.csv 中 數值欄位與installments_payments_num_agg中欄位 合併
atest_num=atest_num.merge(i_num_agg,on='SK_ID_CURR',how='left')# 將bureau.csv的數值欄位與installments_payments_mean合併(307511,49)
#&&&&&&&&&&&將atest_num.csv 中 數值欄位與credit_card_balance_num_agg中欄位 合併
atest_num=atest_num.merge(cc_num_agg,  on='SK_ID_CURR',how='left')# 將bureau.csv的數值欄位與credit_card_balance_mean合併(307511,69)
#&&&&&&&&&&&將atest_num.csv 中 數值欄位與previous_application_num_agg中欄位 合併
atest_num=atest_num.merge(p_num_agg, on='SK_ID_CURR',how='left')# 將bureau.csv的數值欄位與previous_application_num_mean合併(307511,82)


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
############讀取POS_CASH_balance.csv 中類別欄位 
PC_cate=PC[['SK_ID_CURR','NAME_CONTRACT_STATUS']]# 讀取POS_CASH_balance.csv 中的類別欄位'MONTHS_BALANCE' 並保留Key'SK_ID_PREV' 'SK_ID_CURR'
PC_cate=pd.get_dummies(PC_cate)
############計算POS_CASH_balance.csv 中類別欄位的平均值與加總
PC_cate_agg=PC_cate.groupby('SK_ID_CURR').agg('mean')# 將POS_CASH_balance.csv 中的類別欄位 以Key'SK_ID_PREV'分群 並計算平均數
############讀取credit_card_balance.csv 中類別欄位 
cc_cate=cc[['SK_ID_CURR','NAME_CONTRACT_STATUS']]# 讀取credit_card_balance.csv 中的類別欄位 並保留Key'SK_ID_PREV' 'SK_ID_CURR'
cc_cate=pd.get_dummies(cc_cate)
############計算credit_card_balance.csv 中類別欄位的平均值與加總
cc_cate_agg=cc_cate.groupby('SK_ID_CURR').agg('mean')# 將credit_card_balance.csv 中的類別欄位 以Key'SK_ID_PREV'分群 並計算平均數
############分別讀取previous_application.csv 中 Boolean欄位 類別欄位 與 類別欄位(會轉成虛擬變數欄位)
p_cate=previous_application[['SK_ID_PREV','SK_ID_CURR','NAME_CONTRACT_TYPE','WEEKDAY_APPR_PROCESS_START','NAME_CASH_LOAN_PURPOSE','NAME_CONTRACT_STATUS','NAME_PAYMENT_TYPE','CODE_REJECT_REASON','NAME_CLIENT_TYPE','NAME_GOODS_CATEGORY','NAME_PORTFOLIO','NAME_PRODUCT_TYPE','CHANNEL_TYPE','NAME_SELLER_INDUSTRY','NAME_YIELD_GROUP','PRODUCT_COMBINATION']]# 讀取previous_application.csv 中的類別欄位 並保留Key'SK_ID_PREV' 'SK_ID_CURR'()
p_cate=pd.get_dummies(p_cate)
############計算previous_application.csv 中類別欄位的平均值與加總
p_cate_agg=p_cate.groupby('SK_ID_CURR').agg('mean')# 將previous_application.csv 中的類別欄位 以Key'SK_ID_previous_application'分群 並計算平均數
############讀取application_train.csv 中 類別欄位         
at_cate=at[['SK_ID_CURR','NAME_TYPE_SUITE',	'NAME_INCOME_TYPE',	'NAME_EDUCATION_TYPE',	'NAME_FAMILY_STATUS',	'NAME_HOUSING_TYPE',	'OCCUPATION_TYPE',	'WEEKDAY_APPR_PROCESS_START',	'ORGANIZATION_TYPE']]

#&&&&&&&&&&&將application_train.csv 中 類別欄位與bureau_cate_agg中欄位 合併
at_cate=at.merge(b_cate_agg,on='SK_ID_CURR',how='left')# 將bureau.csv的類別欄位與bureau_cate_mean合併(307511,105)
#&&&&&&&&&&&將application_train.csv 中 類別欄位與POS_CASH_balance_cate_agg中欄位 合併
at_cate=at.merge(PC_cate_agg,on='SK_ID_CURR',how='left')# 將bureau.csv的類別欄位與POS_CASH_balance_cate合併(307511,82)
#&&&&&&&&&&&將at.csv 中 類別欄位與credit_card_balance_cate_agg中欄位 合併
at_cate=at.merge(cc_cate_agg,  on='SK_ID_CURR',how='left')# 將bureau.csv的類別欄位與credit_card_balance_mean合併(307511,80)
#&&&&&&&&&&&將at.csv 中 類別欄位與previous_application_cate_agg中欄位 合併
at_cate=at.merge(p_cate_agg, on='SK_ID_CURR',how='left')# 將bureau.csv的類別欄位與previous_application_cate_mean合併(307511,208)

atest_cate=atest[['SK_ID_CURR','NAME_TYPE_SUITE',	'NAME_INCOME_TYPE',	'NAME_EDUCATION_TYPE',	'NAME_FAMILY_STATUS',	'NAME_HOUSING_TYPE',	'OCCUPATION_TYPE',	'WEEKDAY_APPR_PROCESS_START',	'ORGANIZATION_TYPE']]
#(48744,72)
#&&&&&&&&&&&將application_train.csv 中 類別欄位與bureau_cate_agg中欄位 合併
atest_cate=atest_cate.merge(p_cate_agg, on='SK_ID_CURR',how='left')# 將bureau.csv的類別欄位與previous_application_cate_mean合併(307511,208)
atest_cate=atest_cate.merge(b_cate_agg,on='SK_ID_CURR',how='left')# 將bureau.csv的類別欄位與bureau_cate_mean合併(307511,105)
#&&&&&&&&&&&將application_train.csv 中 類別欄位與POS_CASH_balance_cate_agg中欄位 合併
atest_cate=atest_cate.merge(PC_cate_agg,on='SK_ID_CURR',how='left')# 將bureau.csv的類別欄位與POS_CASH_balance_cate合併(307511,82)
#&&&&&&&&&&&將at_cate.csv 中 類別欄位與credit_card_balance_cate_agg中欄位 合併
atest_cate=atest_cate.merge(cc_cate_agg,  on='SK_ID_CURR',how='left')# 將bureau.csv的類別欄位與credit_card_balance_mean合併(48774,82)
#&&&&&&&&&&&將at_cate.csv 中 類別欄位與previous_application_cate_agg中欄位 合併

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
]]#(307511,37)
#&&&&&&&&&&&將application_test..csv 中布林欄位與previous_application_cate_agg中欄位合併
atest_boolean=atest_boolean.merge(p_boolean, on='SK_ID_CURR',how='left')# 將bureau.csv的類別欄位與previous_application_cate_mean合併(48744,40)

############################################################################################
#                      欄位合併/刪除有遺漏值的行/匯出.csv
############################################################################################
at_merge=at_boolean.merge(at_num, on='SK_ID_CURR',how='left')#(307511,125)
at_merge=at_merge.merge(at_cate, on='SK_ID_CURR',how='left')#(307511,332)
at_merge.dropna(how='any', inplace=True)#(158281,332)
at_merge.to_csv('at_merge.csv')
atest_merge=atest_boolean.merge(atest_num, on='SK_ID_CURR',how='left')#(48744,121)
atest_merge=atest_merge.merge(atest_cate, on='SK_ID_CURR',how='left')#(48744,312)
atest_merge.dropna(how='any', inplace=True)#(5268,332)
atest_merge.to_csv('atest_merge.csv')

############################################################################################
#                      TODO
#############################################################################################
#1.delete test col
#atest_cl=[['SK_ID_CURR','NAME_CONTRACT_TYPE','CODE_GENDER','FLAG_OWN_CAR','FLAG_OWN_REALTY','FLAG_MOBIL','FLAG_EMP_PHONE','FLAG_WORK_PHONE','FLAG_CONT_MOBILE','FLAG_PHONE','FLAG_EMAIL','REG_REGION_NOT_LIVE_REGION','REG_REGION_NOT_WORK_REGION','LIVE_REGION_NOT_WORK_REGION','REG_CITY_NOT_LIVE_CITY','REG_CITY_NOT_WORK_CITY','LIVE_CITY_NOT_WORK_CITY','FLAG_DOCUMENT_2','FLAG_DOCUMENT_3','FLAG_DOCUMENT_4','FLAG_DOCUMENT_5','FLAG_DOCUMENT_6','FLAG_DOCUMENT_7','FLAG_DOCUMENT_8','FLAG_DOCUMENT_9','FLAG_DOCUMENT_10','FLAG_DOCUMENT_11','FLAG_DOCUMENT_12','FLAG_DOCUMENT_13','FLAG_DOCUMENT_14','FLAG_DOCUMENT_15','FLAG_DOCUMENT_16','FLAG_DOCUMENT_17','FLAG_DOCUMENT_18','FLAG_DOCUMENT_19','FLAG_DOCUMENT_20','FLAG_DOCUMENT_21','FLAG_LAST_APPL_PER_CONTRACT','NFLAG_LAST_APPL_IN_DAY','NFLAG_INSURED_ON_APPROVAL','DAYS_REGISTRATION','HOUR_APPR_PROCESS_START','AMT_REQ_CREDIT_BUREAU_WEEK','DEF_60_CNT_SOCIAL_CIRCLE','EXT_SOURCE_3','CNT_CHILDREN','AMT_REQ_CREDIT_BUREAU_QRT','CNT_FAM_MEMBERS','DAYS_ID_PUBLISH','DEF_30_CNT_SOCIAL_CIRCLE','REGION_POPULATION_RELATIVE','EXT_SOURCE_2','AMT_REQ_CREDIT_BUREAU_YEAR','AMT_ANNUITY','OBS_30_CNT_SOCIAL_CIRCLE','DAYS_LAST_PHONE_CHANGE','AMT_CREDIT','DAYS_EMPLOYED','REGION_RATING_CLIENT_W_CITY','AMT_REQ_CREDIT_BUREAU_DAY','AMT_GOODS_PRICE','AMT_REQ_CREDIT_BUREAU_HOUR','OBS_60_CNT_SOCIAL_CIRCLE','REGION_RATING_CLIENT','DAYS_BIRTH','AMT_REQ_CREDIT_BUREAU_MON','AMT_INCOME_TOTAL','('DAYS_CREDIT', 'mean')','('CREDIT_DAY_OVERDUE', 'mean')','('DAYS_CREDIT_ENDDATE', 'mean')','('DAYS_ENDDATE_FACT', 'mean')','('CNT_CREDIT_PROLONG', 'mean')','('AMT_CREDIT_SUM', 'mean')','('AMT_CREDIT_SUM_DEBT', 'mean')','('AMT_CREDIT_SUM_LIMIT', 'mean')','('AMT_CREDIT_SUM_OVERDUE', 'mean')','('DAYS_CREDIT_UPDATE', 'mean')','('MONTHS_BALANCE', 'mean')_x','('CNT_INSTALMENT', 'mean')','('CNT_INSTALMENT_FUTURE', 'mean')','('SK_DPD', 'mean')_x','('SK_DPD_DEF', 'mean')_x','('NUM_INSTALMENT_VERSION', 'mean')','('NUM_INSTALMENT_NUMBER', 'mean')','('DAYS_INSTALMENT', 'mean')','('DAYS_ENTRY_PAYMENT', 'mean')','('AMT_INSTALMENT', 'mean')','('AMT_PAYMENT', 'mean')','('MONTHS_BALANCE', 'mean')','('AMT_BALANCE', 'mean')','('AMT_CREDIT_LIMIT_ACTUAL', 'mean')','('AMT_DRAWINGS_ATM_CURRENT', 'mean')','('AMT_DRAWINGS_CURRENT', 'mean')','('AMT_DRAWINGS_OTHER_CURRENT', 'mean')','('AMT_DRAWINGS_POS_CURRENT', 'mean')','('AMT_INST_MIN_REGULARITY', 'mean')','('AMT_PAYMENT_CURRENT', 'mean')','('AMT_PAYMENT_TOTAL_CURRENT', 'mean')','('AMT_RECEIVABLE_PRINCIPAL', 'mean')','('AMT_RECIVABLE', 'mean')','('AMT_TOTAL_RECEIVABLE', 'mean')','('CNT_DRAWINGS_ATM_CURRENT', 'mean')','('CNT_DRAWINGS_CURRENT', 'mean')','('CNT_DRAWINGS_OTHER_CURRENT', 'mean')','('CNT_DRAWINGS_POS_CURRENT', 'mean')','('CNT_INSTALMENT_MATURE_CUM', 'mean')','('SK_DPD', 'mean')','('SK_DPD_DEF', 'mean')','('AMT_ANNUITY', 'mean')','('AMT_APPLICATION', 'mean')','('AMT_CREDIT', 'mean')','('AMT_GOODS_PRICE', 'mean')','('HOUR_APPR_PROCESS_START', 'mean')','('DAYS_DECISION', 'mean')','('SELLERPLACE_AREA', 'mean')','('CNT_PAYMENT', 'mean')','('DAYS_FIRST_DRAWING', 'mean')','('DAYS_FIRST_DUE', 'mean')','('DAYS_LAST_DUE_1ST_VERSION', 'mean')','('DAYS_LAST_DUE', 'mean')','('DAYS_TERMINATION', 'mean')','NAME_TYPE_SUITE','NAME_INCOME_TYPE','NAME_EDUCATION_TYPE','NAME_FAMILY_STATUS','NAME_HOUSING_TYPE','OCCUPATION_TYPE','WEEKDAY_APPR_PROCESS_START','ORGANIZATION_TYPE','SK_ID_PREV','NAME_CONTRACT_TYPE_Cash loans','NAME_CONTRACT_TYPE_Consumer loans','NAME_CONTRACT_TYPE_Revolving loans','NAME_CONTRACT_TYPE_XNA','WEEKDAY_APPR_PROCESS_START_FRIDAY','WEEKDAY_APPR_PROCESS_START_MONDAY','WEEKDAY_APPR_PROCESS_START_SATURDAY','WEEKDAY_APPR_PROCESS_START_SUNDAY','WEEKDAY_APPR_PROCESS_START_THURSDAY','WEEKDAY_APPR_PROCESS_START_TUESDAY','WEEKDAY_APPR_PROCESS_START_WEDNESDAY','NAME_CASH_LOAN_PURPOSE_Building a house or an annex','NAME_CASH_LOAN_PURPOSE_Business development','NAME_CASH_LOAN_PURPOSE_Buying a garage','NAME_CASH_LOAN_PURPOSE_Buying a holiday home / land','NAME_CASH_LOAN_PURPOSE_Buying a home','NAME_CASH_LOAN_PURPOSE_Buying a new car','NAME_CASH_LOAN_PURPOSE_Buying a used car','NAME_CASH_LOAN_PURPOSE_Car repairs','NAME_CASH_LOAN_PURPOSE_Education','NAME_CASH_LOAN_PURPOSE_Everyday expenses','NAME_CASH_LOAN_PURPOSE_Furniture','NAME_CASH_LOAN_PURPOSE_Gasification / water supply','NAME_CASH_LOAN_PURPOSE_Hobby','NAME_CASH_LOAN_PURPOSE_Journey','NAME_CASH_LOAN_PURPOSE_Medicine','NAME_CASH_LOAN_PURPOSE_Money for a third person','NAME_CASH_LOAN_PURPOSE_Other','NAME_CASH_LOAN_PURPOSE_Payments on other loans','NAME_CASH_LOAN_PURPOSE_Purchase of electronic equipment','NAME_CASH_LOAN_PURPOSE_Refusal to name the goal','NAME_CASH_LOAN_PURPOSE_Repairs','NAME_CASH_LOAN_PURPOSE_Urgent needs','NAME_CASH_LOAN_PURPOSE_Wedding / gift / holiday','NAME_CASH_LOAN_PURPOSE_XAP','NAME_CASH_LOAN_PURPOSE_XNA','NAME_CONTRACT_STATUS_Approved_x','NAME_CONTRACT_STATUS_Canceled_x','NAME_CONTRACT_STATUS_Refused_x','NAME_CONTRACT_STATUS_Unused offer','NAME_PAYMENT_TYPE_Cash through the bank','NAME_PAYMENT_TYPE_Cashless from the account of the employer','NAME_PAYMENT_TYPE_Non-cash from your account','NAME_PAYMENT_TYPE_XNA','CODE_REJECT_REASON_CLIENT','CODE_REJECT_REASON_HC','CODE_REJECT_REASON_LIMIT','CODE_REJECT_REASON_SCO','CODE_REJECT_REASON_SCOFR','CODE_REJECT_REASON_SYSTEM','CODE_REJECT_REASON_VERIF','CODE_REJECT_REASON_XAP','CODE_REJECT_REASON_XNA','NAME_CLIENT_TYPE_New','NAME_CLIENT_TYPE_Refreshed','NAME_CLIENT_TYPE_Repeater','NAME_CLIENT_TYPE_XNA','NAME_GOODS_CATEGORY_Additional Service','NAME_GOODS_CATEGORY_Animals','NAME_GOODS_CATEGORY_Audio/Video','NAME_GOODS_CATEGORY_Auto Accessories','NAME_GOODS_CATEGORY_Clothing and Accessories','NAME_GOODS_CATEGORY_Computers','NAME_GOODS_CATEGORY_Construction Materials','NAME_GOODS_CATEGORY_Consumer Electronics','NAME_GOODS_CATEGORY_Direct Sales','NAME_GOODS_CATEGORY_Education','NAME_GOODS_CATEGORY_Fitness','NAME_GOODS_CATEGORY_Furniture','NAME_GOODS_CATEGORY_Gardening','NAME_GOODS_CATEGORY_Homewares','NAME_GOODS_CATEGORY_House Construction','NAME_GOODS_CATEGORY_Insurance','NAME_GOODS_CATEGORY_Jewelry','NAME_GOODS_CATEGORY_Medical Supplies','NAME_GOODS_CATEGORY_Medicine','NAME_GOODS_CATEGORY_Mobile','NAME_GOODS_CATEGORY_Office Appliances','NAME_GOODS_CATEGORY_Other','NAME_GOODS_CATEGORY_Photo / Cinema Equipment','NAME_GOODS_CATEGORY_Sport and Leisure','NAME_GOODS_CATEGORY_Tourism','NAME_GOODS_CATEGORY_Vehicles','NAME_GOODS_CATEGORY_Weapon','NAME_GOODS_CATEGORY_XNA','NAME_PORTFOLIO_Cards','NAME_PORTFOLIO_Cars','NAME_PORTFOLIO_Cash','NAME_PORTFOLIO_POS','NAME_PORTFOLIO_XNA','NAME_PRODUCT_TYPE_XNA','NAME_PRODUCT_TYPE_walk-in','NAME_PRODUCT_TYPE_x-sell','CHANNEL_TYPE_AP+ (Cash loan)','CHANNEL_TYPE_Car dealer','CHANNEL_TYPE_Channel of corporate sales','CHANNEL_TYPE_Contact center','CHANNEL_TYPE_Country-wide','CHANNEL_TYPE_Credit and cash offices','CHANNEL_TYPE_Regional / Local','CHANNEL_TYPE_Stone','NAME_SELLER_INDUSTRY_Auto technology','NAME_SELLER_INDUSTRY_Clothing','NAME_SELLER_INDUSTRY_Connectivity','NAME_SELLER_INDUSTRY_Construction','NAME_SELLER_INDUSTRY_Consumer electronics','NAME_SELLER_INDUSTRY_Furniture','NAME_SELLER_INDUSTRY_Industry','NAME_SELLER_INDUSTRY_Jewelry','NAME_SELLER_INDUSTRY_MLM partners','NAME_SELLER_INDUSTRY_Tourism','NAME_SELLER_INDUSTRY_XNA','NAME_YIELD_GROUP_XNA','NAME_YIELD_GROUP_high','NAME_YIELD_GROUP_low_action','NAME_YIELD_GROUP_low_normal','NAME_YIELD_GROUP_middle','PRODUCT_COMBINATION_Card Street','PRODUCT_COMBINATION_Card X-Sell','PRODUCT_COMBINATION_Cash','PRODUCT_COMBINATION_Cash Street: high','PRODUCT_COMBINATION_Cash Street: low','PRODUCT_COMBINATION_Cash Street: middle','PRODUCT_COMBINATION_Cash X-Sell: high','PRODUCT_COMBINATION_Cash X-Sell: low','PRODUCT_COMBINATION_Cash X-Sell: middle','PRODUCT_COMBINATION_POS household with interest','PRODUCT_COMBINATION_POS household without interest','PRODUCT_COMBINATION_POS industry with interest','PRODUCT_COMBINATION_POS industry without interest','PRODUCT_COMBINATION_POS mobile with interest','PRODUCT_COMBINATION_POS mobile without interest','PRODUCT_COMBINATION_POS other with interest','PRODUCT_COMBINATION_POS others without interest','SK_ID_BUREAU','CREDIT_ACTIVE_Active','CREDIT_ACTIVE_Bad debt','CREDIT_ACTIVE_Closed','CREDIT_ACTIVE_Sold','CREDIT_CURRENCY_currency 1','CREDIT_CURRENCY_currency 2','CREDIT_CURRENCY_currency 3','CREDIT_CURRENCY_currency 4','CREDIT_TYPE_Another type of loan','CREDIT_TYPE_Car loan','CREDIT_TYPE_Cash loan (non-earmarked)','CREDIT_TYPE_Consumer credit','CREDIT_TYPE_Credit card','CREDIT_TYPE_Interbank credit','CREDIT_TYPE_Loan for business development','CREDIT_TYPE_Loan for purchase of shares (margin lending)','CREDIT_TYPE_Loan for the purchase of equipment','CREDIT_TYPE_Loan for working capital replenishment','CREDIT_TYPE_Microloan','CREDIT_TYPE_Mobile operator loan','CREDIT_TYPE_Mortgage','CREDIT_TYPE_Real estate loan','CREDIT_TYPE_Unknown type of loan','STATUS_0','STATUS_1','STATUS_2','STATUS_3','STATUS_4','STATUS_5','STATUS_C','STATUS_X','NAME_CONTRACT_STATUS_Active_x','NAME_CONTRACT_STATUS_Amortized debt','NAME_CONTRACT_STATUS_Approved_y','NAME_CONTRACT_STATUS_Canceled_y','NAME_CONTRACT_STATUS_Completed_x','NAME_CONTRACT_STATUS_Demand_x','NAME_CONTRACT_STATUS_Returned to the store','NAME_CONTRACT_STATUS_Signed_x','NAME_CONTRACT_STATUS_XNA','NAME_CONTRACT_STATUS_Active_y','NAME_CONTRACT_STATUS_Approved','NAME_CONTRACT_STATUS_Completed_y','NAME_CONTRACT_STATUS_Demand_y','NAME_CONTRACT_STATUS_Refused_y','NAME_CONTRACT_STATUS_Sent proposal','NAME_CONTRACT_STATUS_Signed_y'
#]]
#2.rename dictionary col
    