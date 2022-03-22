'''
    [summary stats of each ticker with the best feature]
'''
# libraries
import constant
import time
import numpy as np
import pandas as pd
from pickle import load, dump
# import sys

# set path for data
# sys.path.insert(1, '../data')

# tickers list for copper
# tickers_list = pd.read_csv(constant.STATS_PATH[0]) # for copper

# # only take the tickers with 'Index' in it
# tickers_list = tickers_list[tickers_list['ticker'].str.contains(
#     'Index')].iloc[:, 0].drop_duplicates().reset_index(drop=True)
# tickers_list = tickers_list.reset_index()

file_name = 's3_projected_new_2000'

#.................................................................
#### soy
## read database
# tickers = pd.read_pickle(constant.STATS_PATH_SOY[2])
tickers = pd.read_pickle('./data/' + file_name + '_database.pkl')

# get list of tickers from database
tickers = [tickers[i].columns[1] for i in range(len(tickers))] # columns position to change
tickers = pd.DataFrame(tickers, columns=['ticker'])
tickers.reset_index(inplace=True)

# to map
new_map = dict(tickers[['ticker', 'index']].values)

# summary_report from main results
summary_results = pd.read_pickle('./data/' + file_name + '_summary_report.pkl')

# sharpe ratio
# sr = pd.read_csv(constant.STATS_PATH_SOY[3])
sr = pd.read_csv('./data/' + file_name + '_sharpe_ratio.csv')
# sr = sr.iloc[:-2, :] # optional for other data series
sr['best_feature'] = sr.iloc[:, 2:].idxmax(axis=1)

# rearranging columns
col = sr.pop('best_feature')
sr.insert(2, 'best_feature', col)

sr.drop(sr.columns[3:], axis=1, inplace=True)

sr.ticker = sr.ticker.map(
    lambda x: x.rstrip(' sharpe ratio'))

# add cols
sr = pd.concat([sr, pd.DataFrame(columns=constant.STATS_NEWCOLS)])

# index list
index_list = sr['ticker'].map(new_map).to_list()
for i in range(len(index_list)):
    index_list[i] = int(index_list[i])

# feature list
feature_list = sr['best_feature'].to_list()
feature_list = ['my_strat_' + s for s in feature_list]

for i in range(len(sr)):
    sr.iloc[i, 3:] = summary_results[index_list[i]
                                         ].loc[feature_list[i]].values
# sr.to_csv('./data/' + constant.SOY_COMDTY[0] + '_stats.csv', index=False)
sr.to_csv('./data/' + file_name + '_stats.csv', index=False)