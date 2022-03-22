'''
     [quick changes]

'''
# libraries
import func as f
import features
from signals import *
import constant
import time
# import sys
import numpy as np
import pandas as pd
from pickle import load, dump
from pandas.tseries.offsets import BDay
# from math import sqrt
# import matplotlib.pyplot as plt
# import os
# import seaborn as sns
# sns.set_theme(style="darkgrid")
pd.options.mode.chained_assignment = None

startTime = time.time()

# read tickers
all_tickers = pd.read_pickle('./new_data/ss_dd_new_wrangled_data.pkl')

# read in s2 returns
s2_returns = pd.read_csv('./data/soy_daily_returns.csv', usecols=[0, 2])
s2_returns.date = pd.to_datetime(s2_returns.date, format='%d/%m/%Y')
s2_returns.set_index('date', inplace=True)


# store all dfs with returns series
rets = []
for i in range(len(all_tickers)):
    df = all_tickers[i]

    # set index as entry_date to join with s2 returns
    df.set_index(df['entry_date'], inplace=True)
    # df = df.dropna(subset=[df.columns[1]])

    # combine with s2_returns
    df = pd.concat([s2_returns, df], axis=1).reindex(s2_returns.index)

    df = df.loc[df.index > '1994-12-31']

    # set trading dates start
    starting_dates = df.loc[df.index == df.entry_date].index + BDay(1)
    ending_dates = starting_dates[1:]

    # append latest date
    # ending_dates.append(df.index[-1:])

    # reset trading dates start to
    starting_dates = starting_dates[:-1]

    mth_ret_series = []
    for ii in range(len(starting_dates)):
        mth_ret = np.sum(df.loc[(df.index >= starting_dates[ii]) & (
            df.index <= ending_dates[ii])]['S2 Comdty'])
        mth_ret_series.append(mth_ret)
    mth_ret_series.append(0)

    df.loc[~df['mth_ret'].isnull(), ['mth_ret']] = mth_ret_series

    df = df.loc[~df['mth_ret'].isnull()]
    df = df.reset_index(drop=True)

    rets.append(df)

    print(df.columns[2] + ' added')

print('Total tickers processed: ' + str(len(rets)))

with open('./new_data/ss_dd_new_returns_data.pkl', 'wb') as fout:
    dump(rets, fout)

print('Code ran in {0}s'.format(time.time() - startTime))






#-------------------------------------------------------------------------
# combining

for ii in range(len(s3_results)):
    a = s3_results[ii]
    ticker = a.columns[0]
    a = a.iloc[:, 3:6]

    a = a[a['bin_features'].str.contains("yoy")].sort_values(by='bin_features', ascending=True, axis=0).reset_index(drop=True)
    a['bin_cols'] = a['bin_features'] + '_count'
    a = a[['bin_features', 'bin_sr', 'bin_cols', 'bin_count']]

    b = a[['bin_cols', 'bin_count']].copy()
    a = a.drop(['bin_cols', 'bin_count'], axis=1)
    b.rename(columns={'bin_cols': 'bin_features', 'bin_count':'bin_sr'}, inplace=True)

    t = a.append(b).T.reset_index(drop=True)
    t.columns = t.iloc[0]
    t = t.drop(t.index[0])

    l = t.values

    overall_summary.iloc[:, 1:].append(l)