import json
import datetime
import os
import threading
import warnings

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup

warnings.filterwarnings('ignore')

# global vars
industry_list=['工程建设', '交运设备','电子元件', '医药制造']

first_n_funds=5
aggressive_rate=6
threads_num=80

def log(text): print(str(datetime.datetime.now()).split('.')[0],text)

def multi_threads(func,file_list,threads):
    if __name__ == "__main__": 
        steps=int(len(file_list)/(threads))+1
        num=int(len(file_list)/steps)+1

        for k in range(num):
            locals()['t'+str(k)]=threading.Thread(target=func, 
                                                args=([file_list[steps*k:min(steps*(k+1),len(file_list))]])) 
        for k in range(num):
            locals()['t'+str(k)].start() 
        for k in range(num):
            locals()['t'+str(k)].join() 

print('各位！！！')
print('关心的行业来：\t%s'%'，'.join(industry_list))
print('看看你有多激进：\t%s0%%'%aggressive_rate)
print('多线程个数来：\t%s\n'%threads_num)
print('咱只要最牛逼的 %s 个🐔\n'%first_n_funds)

log('开始寻找财富代码！')


# 获取全部符合要求基金的代码（收益率排序前3000）
themes=pd.read_csv('Data/主题代码对照表.csv')
themes=themes[themes.板块.isin(industry_list)]
themes['后缀']=themes['后缀'].apply(lambda x:x[3:])

log('筛选相关行业的基金！')
res=[]
for _,(ind,ending) in themes.iterrows():
    for i in range(30):
        url='http://fund.eastmoney.com/data/FundGuideapi.aspx?dt=4&sd=&ed=&tp=%s&sc=3y&st=desc&pi=%s&pn=100&zf=diy&sh=list'%(ending,i)
        t=requests.get(url).text
        bs=BeautifulSoup(t)
        data=json.loads(t[len('var rankData ='):])['datas']
        if len(data)==0:break
        res.extend([[ind,x.split(',')[0],x.split(',')[1]] for x in data])
    
funds_code_df=pd.DataFrame(res,columns=['行业','基金代码','基金名称'])
funds_code=funds_code_df.基金代码.values
log('咱一共这么多基金： %s'%len(funds_code))

log('更新历史数据来')
# read data
funds_history=pd.read_csv('Data/funds_data.csv')
funds_history['基金代码']=funds_history.基金代码.astype(int).astype(str).transform(lambda x:x.zfill(6))
funds_history['日期']=pd.to_datetime(funds_history['日期'])

today=datetime.datetime.now().date()    

def get_start_date(code):
    last_date=(funds_history[funds_history['基金代码']==code]['日期'].max()).date()
    if str(last_date)=='NaT':
        last_date=today-datetime.timedelta(720)
    return today-last_date,str(last_date+datetime.timedelta(1))

def get_history(funds_code):
    for code in funds_code:
        if '[' in code: continue
        gap_days,start_day=get_start_date(code)
        if datetime.timedelta(5) > gap_days:
            continue
        for i in range(21):
            url='http://quotes.money.163.com/fund/jzzs_%s_%s.html?start=%s&end=%s&sort=TDATE&order=desc'%(code,i,start_day,str(today))
            try:
                res = requests.get(url,timeout=15)
            except:break
            bs4=BeautifulSoup(res.text)
            try:bs4=bs4.find('tbody').find_all('tr')
            except:
                print('%s Failed'%code)
                break
            if len(bs4)==0:break
            try:
                bs4=[x for x in bs4]
                dates=[x.find_all('td')[0].get_text() for x in bs4]
                net=[x.find_all('td')[1].get_text() for x in bs4]
                per_change=[x.find('span').get_text() for x in bs4]
                cc=[code for x in range(len(dates))]
            except:continue
            funds_res.append((np.vstack((cc,dates,net,per_change)).T))

funds_res=[]
multi_threads(get_history,funds_code,threads_num)

funds_res1=[x for x in funds_res if len(x)!=0]
funds_res1=np.array(funds_res1)
result=np.array((None,None,None,None,))
for x in funds_res1:
    result=np.vstack((result,x))

funds_res1=np.array(funds_res1)
funds_res1=[x[0] for x in funds_res1]
try:
    funds_history_new=pd.DataFrame(result,columns=['基金代码','日期','净值','涨跌幅'])
    funds_history_new=funds_history_new.iloc[1:]
    funds_history_new['基金代码']=funds_history_new.基金代码.astype(int).astype(str).transform(lambda x:x.zfill(6))
    funds_history_new['涨跌幅']=funds_history_new.涨跌幅.transform(lambda x:x[:-1]).astype(float)
    funds_history_new['涨跌幅']/=100
    funds_history_new['涨跌幅']+=1
    funds_history_new['净值']=funds_history_new.净值.astype(float)
    funds_history=pd.concat((funds_history,funds_history_new)).sort_values(by=['基金代码','日期'])
    funds_history['日期']=pd.to_datetime(funds_history['日期'])
except:log('没必要更新数据来')


log('计算评价指标来')
revenue_days=[5,10,22,66,120,250]
revenue_cols=['周','半月','月','季','半年','年']
revenue_zip=zip(revenue_days,revenue_cols)
col1=[]
col2=[]

funds_history=funds_history[funds_history.基金代码.isin(funds_code)].sort_values(['基金代码','日期'])
for day in revenue_days:
    funds_history['lag_%s天'%day]=funds_history.groupby('基金代码')['净值'].shift(day)

# 收益率与最大回撤
for day,col in revenue_zip:
    funds_history['%s收益率'%col]=(funds_history['净值']-funds_history['lag_%s天'%day])/funds_history['lag_%s天'%day]*100
    col2.append('%s收益率'%col)
    if not col in ['半年','年']:
        funds_history['%s最大回撤'%col]=funds_history.groupby('基金代码')['净值'].transform(lambda x:(x.rolling(day).max()-x)/x.rolling(day).max()*100)
        col1.append('%s最大回撤'%col)

cols=['基金代码','日期', '净值', '涨跌幅']+col1+col2
funds_history=funds_history[cols]

funds_withdraw=funds_history[funds_history.日期>pd.to_datetime(datetime.datetime.now().date()-datetime.timedelta(days=250))].groupby('基金代码',as_index=False)[col1].max()
aa=funds_history[funds_history.日期>pd.to_datetime(today-datetime.timedelta(250))]
to_del=[str(x).zfill(6) for x in set(aa[aa.月收益率.isnull()].基金代码)]
funds_history=funds_history[~funds_history.基金代码.isin(to_del)]

funds_select=funds_history.sort_values('日期').groupby('基金代码',as_index=False).apply(lambda x:x.iloc[-1])[[x for x in funds_history.columns if x not in col1]]
funds_select['基金代码']=funds_select.基金代码.astype(int).astype(str).transform(lambda x:x.zfill(6))
funds_select=funds_select.merge(funds_withdraw,on='基金代码')
funds_select=funds_select.merge(funds_code_df,on='基金代码',how='left')

funds_select.dropna(subset=['年收益率'],inplace=True)
funds_select.reset_index(drop=True,inplace=True)
for col in col1+col2:
    funds_select[col]=funds_select[col].transform(lambda x:round(x,2))
funds_select.drop_duplicates(inplace=True)

# normalization
for col in col1:
    funds_select['%s_norm_1'%col]=1/funds_select[col]
    funds_select=funds_select[~(funds_select['%s_norm_1'%col]==np.inf)]
    funds_select['%s_norm_1'%col]=funds_select['%s_norm_1'%col]-funds_select['%s_norm_1'%col].min()
    funds_select['%s_norm_1'%col]=funds_select['%s_norm_1'%col]/funds_select['%s_norm_1'%col].max()

for col in col2:
    funds_select['%s_norm_2'%col]=funds_select[col]-funds_select[col].min()
    funds_select['%s_norm_2'%col]=funds_select['%s_norm_2'%col]/funds_select['%s_norm_2'%col].max()


# sharpe
def downward_std(df):
    ls=df.iloc[-180:].values
    return np.std([x for x in ls if x <1][-180:])

funds_history['涨跌幅']=funds_history['涨跌幅'].astype(float)
df_dstd=funds_history.groupby('基金代码',as_index=False)['涨跌幅'].apply(downward_std)
df_dstd['周收益率']=funds_history.groupby('基金代码')['周收益率'].apply(downward_std).values
df_dstd['月收益率']=funds_history.groupby('基金代码')['月收益率'].apply(downward_std).values
df_dstd['季收益率']=funds_history.groupby('基金代码')['季收益率'].apply(downward_std).values
df_dstd.columns=[x.replace('涨跌幅','日下行标准差').replace('收益率','下行标准差') for x in df_dstd.columns]

funds_select=funds_select.merge(df_dstd,on='基金代码')

col3=['周sharpe','月sharpe','季sharpe']
funds_select['周sharpe']=funds_select['周收益率']/funds_select['周下行标准差']
funds_select['月sharpe']=funds_select['月收益率']/funds_select['月下行标准差']
funds_select['季sharpe']=funds_select['季收益率']/funds_select['季下行标准差']

funds_select=funds_select[funds_select['周sharpe']!=np.inf]
funds_select=funds_select[funds_select['月sharpe']!=np.inf]
funds_select=funds_select[funds_select['季sharpe']!=np.inf]

for col in col3:
    funds_select['%s_norm_3'%col]=funds_select[col]-funds_select[col].min()
    funds_select['%s_norm_3'%col]=funds_select['%s_norm_3'%col]/funds_select['%s_norm_3'%col].max()


eval_col1=[x for x in funds_select.columns if 'norm_1' in x]
eval_col2=[x for x in funds_select.columns if 'norm_2' in x]
eval_col3=[x for x in funds_select.columns if 'norm_3' in x]

log('筛选基金来')
# 过滤数据
# funds_select=funds_select[funds_select.月收益率.astype(float)>0]
funds_select=funds_select[funds_select.季收益率.astype(float)>0]
funds_select=funds_select[funds_select.年收益率.astype(float)>0]

# 评分
result=funds_select.copy()

result['控制回撤评分']=np.sum(result[eval_col1],axis=1)
result['控制回撤评分']=result['控制回撤评分'].transform(lambda x:x-x.min())
result['控制回撤评分']=result['控制回撤评分'].transform(lambda x:x/x.max())
result['控制回撤评分']=result['控制回撤评分'].transform(lambda x:x**0.5)
result['控制回撤评分']=(result['控制回撤评分']*100).transform(lambda x:round(x,2))

result['收益率评分']=np.sum(result[eval_col2],axis=1)
result['收益率评分']=result['收益率评分'].transform(lambda x:x-x.min())
result['收益率评分']=result['收益率评分'].transform(lambda x:x/x.max())
result['收益率评分']=result['收益率评分'].transform(lambda x:x**0.5)
result['收益率评分']=(result['收益率评分']*100).transform(lambda x:round(x,2))

result['Sharpe评分']=np.sum(result[eval_col3],axis=1)
result['Sharpe评分']=result['Sharpe评分'].transform(lambda x:x-x.min())
result['Sharpe评分']=result['Sharpe评分'].transform(lambda x:x/x.max())
result['Sharpe评分']=result['Sharpe评分'].transform(lambda x:x**0.5)
result['Sharpe评分']=(result['Sharpe评分']*100).transform(lambda x:round(x,2))

for risk_factor in [x*10 for x in range(1,10)]:
    result['%s%%贪心评分'%risk_factor]=result['控制回撤评分']*(1-risk_factor/100)+result['收益率评分']*risk_factor/100
    result['%s%%贪心评分'%risk_factor]=result['%s%%贪心评分'%risk_factor]-result['%s%%贪心评分'%risk_factor].min()
    result['%s%%贪心评分'%risk_factor]=result['%s%%贪心评分'%risk_factor]/result['%s%%贪心评分'%risk_factor].max()
    result['%s%%贪心评分'%risk_factor]=(result['%s%%贪心评分'%risk_factor]*100).transform(lambda x:round(x,2))


# 整理columns
score_cols=['%s%%贪心评分'%(x*10) for x in range(1,10)]
cols=['基金代码', '基金名称', '行业','控制回撤评分','收益率评分', 
        '周收益率', '月收益率', '季收益率', '半年收益率', '年收益率', 
        '周最大回撤', '月最大回撤', '季最大回撤']+score_cols+['Sharpe评分']
result_t=result[cols].sort_values(by=['%s0%%贪心评分'%aggressive_rate],ascending=False)
result_t=result_t.head(100).sort_values('Sharpe评分',ascending=False).head(first_n_funds*2)

result_t['Sharpe评分']=result_t['Sharpe评分'].transform(lambda x:x-x.min())
result_t['Sharpe评分']=result_t['Sharpe评分'].transform(lambda x:x/x.max())
result_t['Sharpe评分']=result_t['Sharpe评分'].transform(lambda x:x**0.5)
result_t['Sharpe评分']=(result_t['Sharpe评分']*100).transform(lambda x:round(x,2))
result_t.sort_values('Sharpe评分',ascending=False,inplace=True)

result_t=result_t.head(first_n_funds)
result_t['建议持仓']=result_t['Sharpe评分']/result_t['Sharpe评分'].sum()*100

log('今儿个的🐔儿选完了来')
log('最牛逼的 %s 个🐔儿来！'%first_n_funds)
display(result_t[['基金代码','基金名称','行业','控制回撤评分','收益率评分','Sharpe评分',
                    '%s0%%贪心评分'%aggressive_rate,'建议持仓']].reset_index(drop=True))
