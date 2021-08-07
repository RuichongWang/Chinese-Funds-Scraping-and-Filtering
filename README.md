# Chinese-Funds-Scraping-and-Filtering 中国基金爬虫获取历史数据与筛选

主要逻辑是基于动量与最大回撤进行筛选，加入了下行标准差和下行Sharpe进行辅助筛选。

## Repository contents
* Get_and_Merge_Data.py == Data downloading and X_y Split
* Comparison_of_FFT_STFT_and_CWT.ipynb == Comparison of common signal decomposition methods
* Training.py == simple LSTM model based on CWT signal

## Repository summary
### How to use?
- 下载全部公募基金与指数型基金代码(1_基金代码名称对照表.ipynb)，数据来源:[天天基金网](http://fund.eastmoney.com/daogou/)和[网易财经](http://quotes.money.163.com/fn/service/netvalue.php?host=/fn/service/netvalue.php&page=1&query=STYPE:FDO;TYPE3:ZSX&fields=no,PUBLISHDATE,SYMBOL,SNAME,NAV,PCHG,M12RETRUN,SLNAVG,ZJZC&sort=PCHG&order=desc&count=500)。
- 
