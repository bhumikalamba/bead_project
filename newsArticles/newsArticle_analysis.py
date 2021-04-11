import os
from pyspark.sql import SparkSession
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import pytz
from itertools import cycle
import numpy as np



cycol = cycle('bgrcmk')


val = os.walk(os.getcwd()+ "\dl_files")
parquet_paths = list()

for k,v,p in val:
    for _p in p:
        if 'parquet' in _p:
            path = str(k) + '\\' + str(_p)
            parquet_paths.append(path)

spark = SparkSession.builder.master("local").appName('ReadParquet').config("spark.driver.host","localhost").config("spark.ui.port","4040").getOrCreate()
parquetFile = spark.read.parquet(*parquet_paths)

parquetFile.printSchema()


parquetFile.createOrReplaceTempView("parquetFile")
parquetTable = spark.sql("SELECT date_publish,authors,source_domain FROM parquetFile WHERE date_publish > '2021-03-16'")
parquetTable = parquetTable.toPandas()


parquetTable.head()
parquetTable['count'] = 0


def get_bitfinex_hourly():
    df = pd.read_csv('C:\\Users\\suren\\Documents\\bead_project\\bitfinex\\bitfinex_btcusd.csv')
    df['time'] = pd.to_datetime(df['time'])
    df.index = df['time']
    df2 = df[['close']]
    df3 = df2.loc['20210318':]
    df3_hourly = df3.resample('H').mean()
    df3_hourly['lag1'] = df3_hourly['close'].shift(1)
    df3_hourly['variance'] = df3_hourly['close'] - df3_hourly['lag1']
    df3_hourly = df3_hourly.dropna()
    return df3_hourly

df3_hourly = get_bitfinex_hourly()




#print('TYEP',type(df3_hourly['time']))

parquetTable['date_publish'] = pd.to_datetime(parquetTable['date_publish'])
parquetTable['date_publish2'] = parquetTable['date_publish'].dt.floor('h')
#parquetTable.index = parquetTable['date_publish2']
parquetTable['date_publish2'] = pd.to_datetime(parquetTable['date_publish2'])
parquetTableGrouped = parquetTable.groupby(['date_publish','source_domain'])['source_domain'].count().reset_index(name="count")





#parquetTableGrouped = parquetTableGrouped.to_frame()


#parquetTableGrouped.index = parquetTableGrouped['date_publish2']

#print('Wh  the fuck',parquetTableGrouped[parquetTableGrouped['source_domain'] == 'www.coindesk.com'])

domains = parquetTableGrouped.source_domain.unique()


df_var1000 = df3_hourly[abs(df3_hourly['variance'])>1000]


row_count = (df_var1000.shape[0])

for row in df_var1000.itertuples():
    parquetTableGrouped[row[0]] = ((row[0].replace(tzinfo=None).to_pydatetime() - parquetTableGrouped['date_publish']) /np.timedelta64(1, 's'))/86400



#parquetTableGrouped.iloc[:, 3:] = (parquetTableGrouped.iloc[:,3:] / np.timedelta64(1, 's'))/86400
df_list = list()
df_dict = dict()
for i in range(3,row_count+3):
    df_dict['df_'+str(i)] = parquetTableGrouped.iloc[:,i:i+1]
    #df_dict['df_'+str(i)['source_domain']] = parquetTableGrouped.iloc[:, 1:2]


print('hehe',df_dict)
# df = parquetTableGrouped.iloc[:,3:4]
# print('sup sup',parquetTableGrouped['date_publish'].head())
# print(temp/np.timedelta64(1, 's'))
# print('type',type(temp))
# print('type??',temp.dtypes)
# print('aii')


parquetTableGrouped.index = parquetTableGrouped['date_publish']



fig, ax = plt.subplots(figsize=(16,8))
ax.scatter(df_var1000.index,df_var1000 ['variance'])

ax.set_xlabel('Time')
ax.set_ylabel('Bitcoin Hourly Price Variance')

chart_dict = dict()
ax2 = ax.twinx()
#ax3 = ax.twinx()

prop_iter = iter(plt.rcParams['axes.prop_cycle'])

for idx,dm_name in enumerate(domains):
    #index = idx + 2
    #tmp = 'ax' + str(index)
    #chart_dict['ax' + str(index)] = ax.twinx()
    ax2.bar(parquetTableGrouped[parquetTableGrouped['source_domain'] == dm_name].index,
            parquetTableGrouped[parquetTableGrouped['source_domain'] == dm_name]['count'], alpha=0.8,
            width=0.08,color=next(prop_iter)['color'],label=dm_name)
    ax2.legend()
    #chart_dict['ax' + str(index)].grid(None)



# ax2 = ax.twinx()
# ax2.bar(parquetTableGrouped[parquetTableGrouped['source_domain'] == 'www.coindesk.com'].index,parquetTableGrouped[parquetTableGrouped['source_domain'] == 'www.coindesk.com']['count'],alpha=0.3,width=0.08)
# ax2.set_ylabel('Coindesk')
#
# ax3 = ax.twinx()
# ax3.bar(parquetTableGrouped[parquetTableGrouped['source_domain'] == 'www.forbes.com'].index,parquetTableGrouped[parquetTableGrouped['source_domain'] == 'www.forbes.com']['count'],alpha=0.3,width=0.08)
# ax3.set_ylabel('Coindesk')
plt.show()


