'''
     [Main script to get results for bins]

'''
# libraries
import sys
import func as f
import new_signals as ns
import features
from signals import *
# import constant
from datetime import datetime
import time
import numpy as np
import pandas as pd
from pickle import dump
from pandas.tseries.offsets import BDay, DateOffset
import math

startTime = time.time()


# on PROJECTED
s3_new = pd.read_pickle('./new_data/ss_dd_new_returns_data.pkl')


df_list = list()
overall_database = list()

# # iterate through entire list of dataframes
for ii in range(len(s3_new)):
    indic = s3_new[ii]

    if indic.empty:
        continue

    indic = indic.loc[indic.date > '2000-01-01'].reset_index(drop=True)
    indic.iloc[:, [2, 3]] = indic.iloc[:, [2, 3]].replace(0, np.nan)

    # set shift and year
    indic['shifted_year'] = indic['date'].dt.year.shift(-12)
    indic['shifted_' + indic.columns[2]] = indic[indic.columns[2]].shift(-12)

    indic['yoy'] = (
        (indic[indic.columns[7]]/indic[indic.columns[2]]) - 1).shift(12)
    indic = indic.drop(['shifted_year', 'shifted_' + indic.columns[2]], axis=1)

    # run feature
    for i in range(len(ns.MONTHS)):
        indic = ns.ma(indic, ns.MONTHS[i])
        indic = ns.period_minmax(indic, ns.MONTHS[i])
        indic = ns.yoy_ma_minmax(indic, ns.MONTHS[i])

    # run signal bins
    for a in range(len(ns.MINMAX_FEATURES)):
        indic = ns.signal_bins(indic, ns.MINMAX_FEATURES[a])

    # signal no changes
    for b in range(len(ns.MA_FEATURES)):
        for bi in range(len(ns.MINMAX_FEATURES)):
            indic = ns.signal_no_changes(indic, ns.MA_FEATURES[b], 0, 0)
            indic = ns.signal_no_changes(
                indic, ns.MINMAX_FEATURES[bi], 0.5, 0.5)

    # strat returns
    for c in range(len(ns.MINMAX_FEATURES)):
        indic = ns.my_strat_bins(indic, ns.MINMAX_FEATURES[c])

    # for signal no changes
    for d in range(len(ns.MA_FEATURES)):
        for di in range(len(ns.MINMAX_FEATURES)):
            indic = ns.my_strat(indic, ns.MA_FEATURES[d])
            indic = ns.my_strat(indic, ns.MINMAX_FEATURES[di])

    bin_no = ['1', '2', '3', '4']

    all_count = []
    all_sr_values = []

    bin_features = []
    minmax_count = []
    minmax_sr = []

    # minmax_count
    for e in range(len(bin_no)):
        for ei in range(len(ns.MINMAX_FEATURES)):
            indic['my_strat_' + ns.MINMAX_FEATURES[ei] + '_bins' +
                  bin_no[e]].replace(0.0, np.nan, inplace=True)
            minmax_count.append(
                indic['my_strat_' + ns.MINMAX_FEATURES[ei] + '_bins' + bin_no[e]].count())
            sr = (indic['my_strat_' + ns.MINMAX_FEATURES[ei] + '_bins' + bin_no[e]].mean() *
                  math.sqrt(12))/indic['my_strat_' + ns.MINMAX_FEATURES[ei] + '_bins' + bin_no[e]].std()
            minmax_sr.append(sr)
            s = ns.MINMAX_FEATURES[ei] + '_bin' + bin_no[e]
            bin_features.append(s)

    #  count, sr_values
    for f in range(len(ns.ALL_FEATURES)):
        indic['my_strat_' + ns.ALL_FEATURES[f]
              ].replace(0.0, np.nan, inplace=True)
        all_count.append(indic['my_strat_' + ns.ALL_FEATURES[f]].count())
        all_sr_values.append((indic['my_strat_' + ns.ALL_FEATURES[f]].mean()
                              * math.sqrt(12))/indic['my_strat_' + ns.ALL_FEATURES[f]].std())

    d1 = pd.DataFrame({
        indic.columns[2]: ns.ALL_FEATURES,
        'all_sr': pd.Series(all_sr_values),
        'all_count': pd.Series(all_count),
    })

    d2 = pd.DataFrame({
        'bin_features': pd.Series(bin_features),
        'bin_sr': pd.Series(minmax_sr),
        'bin_count': pd.Series(minmax_count)
    })

    d = pd.concat([d1, d2], axis=1)


    print(indic.columns[2])

    print('block ran in {0}s'.format(time.time() - startTime))

    df_list.append(d)
    overall_database.append(indic)

    print('moving to next dataset')
    print(str(ii) + ' iteration')


# define file name
file_name = 's3_proj_bins'

# dump into pickle file
if __name__ == "__main__":
    # unwrangled results
    with open('./new_data/' + file_name + '_results.pkl', 'wb') as fout:
        dump(df_list, fout)

    with open('./new_data/' + file_name + '_database.pkl', 'wb') as fout:
        dump(overall_database, fout)

    print('code finished')
