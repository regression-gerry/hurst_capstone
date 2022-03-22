'''
     [script for singular parse;
     
     - e.g. ENSO data on multiple commodities
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
import gc

startTime = time.time()

# 1. read in data files
# read tickers
# folder = 'data'
# file_name = 'soy_w_tickers'
ticker_df = pd.read_csv('./new_data/NOAANT34 ANOM Index.csv')

# analysis on singular ticker against all the comdty returns for sharpe ratio
# set main df
df = ticker_df.iloc[:, 1:]
ret_column = df.columns[3:].to_list() # select comdty strings 

# wrangle for week
df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
df['week'] = df['date'].dt.week
df.replace({'week': 53}, 52, inplace=True)

df['yoy'] = df.groupby(['week'])[df.columns[1]].pct_change()
df = df.drop(['week'], axis=1)

# minmax on the yoy -> requires the yoy column beforehand
df = ns.yoy_minmax(df, 'wk', ns.WEEKS[0], ns.WEEKS_D[0])

# different period minmax (52, 104, 156, 260 weeks)
for i, j in zip(ns.WEEKS, ns.WEEKS_D):
    col_no = 1
    df = ns.period_minmax(df, 'wk', col_no, i, j)


# create variables based off the returns and parse through it
# apply signals (1, 0) and strat returns
for features in range(len(ns.WK_MINMAX_FEATURES)):
    df = ns.signal_bins(df, ns.WK_MINMAX_FEATURES[features])
    for ret in range(len(ret_column)):
        df = ns.singular_strat_bins(
            df, ns.WK_MINMAX_FEATURES[features], ret_column[ret])


# finding the exact bins
bin_no = ['1', '2', '3', '4']

bin_features = []
minmax_count = []
minmax_sr = []

# minmax_count and minmax_sr
for i_bin in range(len(bin_no)):
    for j_minmax in range(len(ns.WK_MINMAX_FEATURES)):
        for k_ret in range(len(ret_column)):
            df['my_strat_' + ns.WK_MINMAX_FEATURES[j_minmax] + ret_column[k_ret] + '_bins' +
                bin_no[i_bin]].replace(0.0, np.nan, inplace=True)
            minmax_count.append(
                df['my_strat_' + ns.WK_MINMAX_FEATURES[j_minmax] + ret_column[k_ret] + '_bins' + bin_no[i_bin]].count())

            # sharpe ratio
            sr = (df['my_strat_' + ns.WK_MINMAX_FEATURES[j_minmax] + ret_column[k_ret] + '_bins' + bin_no[i_bin]].mean() *
                  math.sqrt(12))/df['my_strat_' + ns.WK_MINMAX_FEATURES[j_minmax] + ret_column[k_ret] + '_bins' + bin_no[i_bin]].std()
            minmax_sr.append(sr)

            # bin features string
            s = ns.WK_MINMAX_FEATURES[j_minmax] + \
                ret_column[k_ret] + '_bin' + bin_no[i_bin]
            bin_features.append(s)


# dataframe for storing sr results
d2 = pd.DataFrame({
    df.columns[1]: pd.Series([], dtype=pd.StringDtype()),
    'bin_features': pd.Series(bin_features),
    'bin_sr': pd.Series(minmax_sr),
    'bin_count': pd.Series(minmax_count)
})

d2 = d2.sort_values(by='bin_features', ascending=True,
                    axis=0).reset_index(drop=True)

d2 = d2[~(d2.bin_features.str.contains("bin2|bin3")) & (d2.bin_count > 50)]

d2.to_csv('./new_data/enso34anomreturns.csv')
df.to_csv('./new_data/enso34anom_wrangled.csv')
gc.collect()
