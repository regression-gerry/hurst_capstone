'''
    [Get weekly stats and export dfs into summary sheet]
'''

# import libraries
import new_signals as ns
from datetime import datetime
import time
import numpy as np
import pandas as pd
import pickle
from pandas.tseries.offsets import BDay, DateOffset
import math

# import sharpe ratio unwrangled file
comdty = ['alum', 'soy_s2', 'soymeal_sm2', 'soyoil_b02']
# folder = 'new_data'
# file_name = comdty[0] + '_w_sr_bins'
# soy_sr = pd.read_pickle('./' + folder + '/' + file_name + '.pkl')

soy_sr = pd.read_pickle('./new_data/sr_60_long_alum.pkl')


# create empty dataframe with columns
bins = ['1', '2', '3', '4']
WK_MINMAX_FEATURES = [
    '52wk_minmax', 'yoy_52wk_minmax',
    '104wk_minmax', '156wk_minmax',
    '260wk_minmax'
]

WK_MINMAX_FEATURES = [WK_MINMAX_FEATURES[j] + "_bin" + bins[i]
                      for i in range(len(bins)) for j in range(len(WK_MINMAX_FEATURES))]

WK_MINMAX_FEATURES = sorted(WK_MINMAX_FEATURES)

WK_MINMAX_FEATURES_COUNT = [WK_MINMAX_FEATURES[i] +
                            "_count" for i in range(len(WK_MINMAX_FEATURES))]
WK_MINMAX_FEATURES_COUNT = sorted(WK_MINMAX_FEATURES_COUNT)

for i in range(len(WK_MINMAX_FEATURES_COUNT)):
    WK_MINMAX_FEATURES.append(WK_MINMAX_FEATURES_COUNT[i])

WK_MINMAX_FEATURES.insert(0, 'ticker')

sr_stats = pd.DataFrame(columns=WK_MINMAX_FEATURES)

# tickers to be included in df
ww_tickers = []
for i in range(len(soy_sr)):
    if soy_sr[i].empty:
        continue
    else:
        ww_tickers.append(soy_sr[i].columns[0])

# wrangle portion
for i in range(len(soy_sr)):
    if soy_sr[i].empty:
        continue
    # wrangle sharpe ratio
    sr = soy_sr[i].iloc[:, 1:3].reset_index(drop=True)
    sr = sr.T.reset_index(drop=True)
    sr.columns = sr.iloc[0]
    sr = sr.drop(sr.index[0]).reset_index(drop=True)

    # wrangle the bin count -> no. of trades
    bin_count = soy_sr[i].iloc[:, [1, 3]].reset_index(drop=True)
    bin_count['bin_features'] = bin_count['bin_features'].apply(
        lambda x: x + '_count')
    bin_count = bin_count.T.reset_index(drop=True)
    bin_count.columns = bin_count.iloc[0]
    bin_count = bin_count.drop(bin_count.index[0]).reset_index(drop=True)
    sr_bin = pd.concat([bin_count, sr], axis=1).reindex(bin_count.index)

    # append to the empty df
    sr_stats = sr_stats.append([sr_bin])

# add the tickers last
sr_stats.ticker = ww_tickers

if __name__ == "__main__":
    # sr_stats.to_csv('./new_data/' + comdty[0] + '_w_data_stats.csv')
    sr_stats.to_csv('./new_data/alum_260_w_data_stats.csv')
