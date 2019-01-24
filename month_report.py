# -*- coding: utf-8 -*-

import pandas as pd

data = 'D:\\数据\\代发数据分析\\2018年12月\\sjfx.xlsx'
data = data.decode('utf-8')
result = 'D:\\数据\\代发数据分析\\2018年12月\\result.xlsx'
result = result.decode('utf-8')
result_1 = 'D:\\数据\\代发数据分析\\2018年12月\\result_1.xlsx'
result_1 = result_1.decode('utf-8')
result_2 = 'D:\\数据\\代发数据分析\\2018年12月\\result_2.xlsx'
result_2 = result_2.decode('utf-8')
mer_name = '商户名称'
mer_name = mer_name.decode('utf-8')
mer_no = '商户号'
mer_no = mer_no.decode('utf-8')
time = '交易时间'
time = time.decode('utf-8')
state = '状态'
state = state.decode('utf-8')
state_succ = '成功'
state_succ = state_succ.decode('utf-8')
state_fail = '失败'
state_fail = state_fail.decode('utf-8')
state_tp = '退票'
state_tp = state_tp.decode('utf-8')
bank = '收款方银行名称'
bank = bank.decode('utf-8')
way_name = '通道名称'
way_name = way_name.decode('utf-8')
account_type = '收款账户类型'
account_type = account_type.decode('utf-8')
account_type_jieji = '借记卡'
account_type_jieji = account_type_jieji.decode('utf-8')
account_type_xinyongka = '信用卡'
account_type_xinyongka = account_type_xinyongka.decode('utf-8')
card_type = '收款银行卡类型'
card_type = card_type.decode('utf-8')
card_type_duigong = '对公'
card_type_duigong = card_type_duigong.decode('utf-8')
card_type_duisi = '对私'
card_type_duisi = card_type_duisi.decode('utf-8')
amount = '金额'
amount = amount.decode('utf-8')

#-------读取数据-------#
daifa = pd.read_excel(data, sheet_name = 'daifa')
daifa_card = daifa.ix[daifa[bank].notnull()] #ix()先选行，再选列
chongzhi = pd.read_excel(data, sheet_name = 'chongzhi')

writer = pd.ExcelWriter(result_1)#同时保存多个sheet所需

#-------按时间统计-------#
#每日金额汇总
daifa_day_amount = daifa_card.ix[daifa[state] == state_succ].groupby([time])[amount].sum().to_frame()

#每日交易笔数、失败率、退票率
daifa_day_prob = pd.crosstab(daifa_card[time], daifa_card[state], margins=True)
daifa_day_prob['daifa_day_fail_prob'] = daifa_day_prob[state_fail] / daifa_day_prob['All']
daifa_day_prob['daifa_day_tp_prob'] = daifa_day_prob[state_tp] / daifa_day_prob['All']

daifa_day_prob = daifa_day_prob[['daifa_day_fail_prob', 'daifa_day_tp_prob']]

pd.merge(daifa_day_amount, daifa_day_prob, how='right', right_index=True, left_index = True).to_excel(writer, sheet_name='time')

#-------按商户统计-------#

#商户金额汇总
daifa_mer_amount = daifa_card.ix[daifa[state] == state_succ].groupby([mer_name])[amount].sum().to_frame()

#商户交易笔数、失败率、退票率
daifa_mer_prob = pd.crosstab(daifa_card[mer_name], daifa_card[state], margins=True)
daifa_mer_prob['daifa_mer_fail_prob'] = daifa_mer_prob[state_fail] / daifa_mer_prob['All']
daifa_mer_prob['daifa_mer_tp_prob'] = daifa_mer_prob[state_tp] / daifa_mer_prob['All']

daifa_mer_prob = daifa_mer_prob[[state_succ, 'daifa_mer_fail_prob', 'daifa_mer_tp_prob']]

result_mer = pd.merge(daifa_mer_amount, daifa_mer_prob, how='right', right_index=True, left_index = True)

result_table = 0
flag = 0

def type_data(type, type_type):
	
	global flag, result_table
	
	daifa_mer_type = daifa_card.ix[daifa[type_type] == type].ix[daifa[state] == state_succ]
	
	amount_type = daifa_mer_type.groupby([mer_name])[amount].sum().to_frame()
	amount_type.columns = ['%s_amount' %type]
	
	#count_type = pd.crosstab(daifa_mer_type[mer_name], daifa_mer_type[state])
	#count_type.columns = ['%s_count' %type] 
	#无成功交易则透视表该列为空，无法为列赋值
	
	count_type = daifa_mer_type.groupby([mer_name])[state].count().to_frame()
	count_type.columns = ['%s_count' %type]
	
	mix_table = pd.merge(amount_type, count_type, how='outer', left_index=True, right_index=True)
		
	if flag:
		result_table = pd.merge(result_table, mix_table, how='outer', left_index=True, right_index=True)
	else:
		result_table = mix_table
		flag+=1
	
	return result_table, flag
	
for i in [account_type_jieji, account_type_xinyongka, card_type_duigong, card_type_duisi]:
	
	if i == account_type_jieji or i == account_type_xinyongka:
		type_data(i, account_type)
	else:
		type_data(i, card_type)


pd.merge(result_table, result_mer, left_index=True, right_index=True, how='right').to_excel(writer, sheet_name='mer')

#-------按通道统计-------#
daifa_way_amount = daifa_card.ix[daifa[state] == state_succ].groupby([way_name])[amount].sum().to_frame()

daifa_way_prob = pd.crosstab(daifa_card[way_name], daifa_card[state], margins=True)
daifa_way_prob['daifa_way_fail_prob'] = daifa_way_prob[state_fail] / daifa_way_prob['All']
daifa_way_prob['daifa_way_tp_prob'] = daifa_way_prob[state_tp] / daifa_way_prob['All']

daifa_way_prob = daifa_way_prob[[state_succ, 'daifa_way_fail_prob', 'daifa_way_tp_prob']]

pd.merge(daifa_way_amount, daifa_way_prob, how='right', right_index=True, left_index = True).to_excel(writer,sheet_name='way')

writer.save()

#-------在充值数据内填充商户号-------#
#daifa[[mer_name,mer_no]].drop_duplicates()
#print daifa
#dc = pd.merge(daifa, chongzhi, on = mer_no, how = 'right').to_excel(result)

###################################################################################################
#---------------------------------------------总结-------------------------------------------------
#1、数据透视表
#--pd.crosstab(行, 列, margins=true *代表有小计)
#--groupby(行)[值].sum()
#--pivot_table(值, index=行, columns=列, aggfunc='max'（函数可自定义）, margins=True, fill_value=0)
#空表使用groupby会保留列名，crosstab不会
#
#2、ix[行筛选，列筛选]
#
#3、中文用decode('utf-8')转码
#
#4、在同一张表中插入多个sheet
#	writer = pd.ExcelWriter(result_1)
#	a.to_excel(writer, sheet_name='sheet1')
#	writer.save()
#
#5、两列间计算插入到新列
#dataframe['newcolumn'] = dataframe['column1'] / dataframe['column2']
#
#6、列重命名
#dataframe.columns['name1', 'name2', ...]
#
#7、函数内声明全局变量
#global v1,v2,v3,...
#
####################################################################################################

