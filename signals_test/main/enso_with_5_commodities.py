'''
    [ENSO horizontal analysis
        
    pd.dateoffset + 5days for initial date column
    ]
'''

# libraries
import new_signals as ns
from datetime import datetime
import time
import numpy as np
import pandas as pd
import pickle
from pandas.tseries.offsets import BDay, DateOffset
import math

startTime = time.time()

# 1. read in data files
# read tickers
folder = 'new_data'
file_name = 'enso_1990'
all_tickers = pd.read_pickle('./' + folder + '/' + file_name + '.pkl')

# read in comdty returns
comdty_cols = [
    0, 2, 16, 20, 6, 10, 13, 
    28, 32, 35, 38, 42, 45, 
    48, 52, 56, 60, 64
]

ret = ns.comdty_parser('new_data', 'comdty_ret', comdty_cols)
# print(ret.columns)

dfs = []
for i in range(0, len(all_tickers)):
    df = all_tickers[i]

    # resample and fill nan for missing dates
    df = df.set_index(df.columns[0]).resample(
        'W-{:%a}'.format(df.date[0])).asfreq().fillna(np.nan).reset_index()

    # minus 5 days
    df.date = df.date + pd.DateOffset(5)

    # set entry date for trading date
    df['entry_date'] = df['date'] + BDay(1)

    # df['week'] = df['date'].apply(lambda dt: int(math.ceil(dt.day/7.0)))
    # df['month'] = df['date'].dt.month
    # df['yoy'] = df.groupby(['month', 'week'])[df.columns[1]].pct_change()

    ### set arbitrary returns columns
    for i in range(len(ret.columns)):
        df[ret.columns[i] + '_ret'] = np.random.randint(100)

    df.set_index(df['entry_date'], inplace=True)

    # combine dataframes
    df = pd.concat([ret, df], axis=1).reindex(ret.index)

    ### start and end date range
    start = df.loc[df.index == df.entry_date].index + BDay(1)
    end = df.loc[df.index == df.entry_date].index[1:]
    # add the last value of the daily returns
    end = end.append(ret.index[-1:])

    ### all returns
    all_ret = [[np.sum(df.loc[(df.index >= start[i]) & (
                df.index <= end[i])][df.columns[j]]) for i in range(len(start))] for j in range(len(ret.columns))]

    # store initial comdty returns columns as list
    ret_list = ret.columns.to_list()

    ### parallel for loop
    for returns, pos in zip(all_ret, ret_list):
        df.loc[~df[pos + '_ret'].isnull(), [pos + '_ret']] = returns

    # select either comdty returns that is null and return the dataframe
    df = df.loc[~df[ret_list[0] + '_ret'].isnull()]
    df = df.reset_index(drop=True)
    df = df.drop(columns=ret.columns.to_list(), axis=1)

    ### date,0|ticker,1|entry_date,2
    # make it absolute
    df.iloc[:, 3:] = df.iloc[:, 3:] / 100
    dfs.append(df)
    print(df.columns[1])
    print('block ran in {0}s'.format(time.time() - startTime))


for i in range(len(dfs)):
    dfs[i].to_csv('./new_data/' + dfs[i].columns[1] + '.csv')


with open('./new_data/enso_1990_wrangled.pkl', 'wb') as f:
    pickle.dump(dfs, f)

print('code ran in {0}s'.format(time.time() - startTime))