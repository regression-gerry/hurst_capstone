'''
     [Main script to get results]

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
from math import sqrt
import matplotlib.pyplot as plt
import os
import seaborn as sns
sns.set_theme(style="darkgrid")
pd.options.mode.chained_assignment = None

startTime = time.time()

# set path for scripts
# sys.path.insert(1, '../data')

# set path to data
# all_tickers = pd.read_pickle(constant.SOY_TICKERS_PATH[0])

# on PROJECTED
all_tickers = pd.read_pickle('./data/soy_ss_dd_estimated.pkl')


# setting comdty dataframe to map to
com = pd.read_pickle(constant.SOY_COM_DATA[0])  # SM3 or Soybean meal comdty

# com = pd.read_pickle('./data/LP3 COMDTY.pkl')
# mapping = dict(com[['date', 'LA3 COMDTY']].values)
mapping = dict(com.values)  # set mapping

# str of comdty
comdty = com.columns[1]

# stores the results of the features into a dictionary
results = {}

df_list = list()
overall_database = list()

# # iterate through entire list of dataframes
for j in range(len(all_tickers)):

    # indic :: [['period_end, 'ticker', 'release_date']]
    indic = all_tickers[j]

    if indic.empty:
        continue

    # if indic.empty:
    #     continue
    # else:
    #     pass

    # substring = 'Index'

    # if substring not in indic.columns[1]:
    #     continue

    # ............................................................
    # to deal with weekly dates
    freq = pd.infer_freq(indic[indic.columns[0]])
    if freq is None:
        freq = 'M'
    else:
        pass

    # resample and fill missing values
    indic = indic.set_index(indic.columns[0]).resample(
        freq).asfreq().fillna(np.nan).reset_index()

    # # find the difference in days for the last two points
    # d = indic[indic.columns[0]][-1:].values - indic[indic.columns[0]][-2:-1].values
    # diff_days = int((d/np.timedelta64(1, 'D')).item(0))

    # # if difference in days is > 8 -> weekly data, else mnonthly
    # if diff_days > 8:
    #     freq = 'M'
    # elif freq is None:
    #     freq = 'M'
    # ...........................................................
    # elif diff.days > 20:
    #     freq = 'M'
    # else:
    #     pass

    # drop any duplicates from release dates
    # indic = indic.drop_duplicates(subset=[indic.columns[2]], keep='last')

    # changed to 12th day of the month
    indic[indic.columns[0]] = indic[indic.columns[0]].apply(
        lambda dt: dt.replace(day=12))

    # manually add 10 days to release days
    # indic['release_date'] = indic[indic.columns[0]] + pd.DateOffset(12)

    # add entry dates based on release dates
    indic['entry_date'] = indic[[indic.columns[0]]] + \
        BDay(1)  # column 0 to period end, initial = 2

    indic = f.get_entry_price(indic, mapping)
    indic = f.get_exit_price(indic, mapping, 1)

    # indicator str
    indicator = indic.columns[1]

    print(indicator, comdty)

    print('block ran in {0}s'.format(time.time() - startTime))

    # # no. of periods in a year; 12 = monthly, > 12 either weekly or daily
    # n_period = indic[indic.columns[0]].groupby(
    #     indic[indic.columns[0]].dt.to_period("Y")).agg('count').max()

    # manually define periods
    # yr_period = 12

    # # no. of periods for 6 months
    # mn_period = int(yr_period / 2)

    # # freq of t/s
    # freq = pd.infer_freq(indic[indic.columns[0]])

    # # check time difference to determine freq if freq is unavailable
    # any random data points
    # diff = indic[indic.columns[0]][11] - indic[indic.columns[0]][10]

    indic['mth_ret'] = indic['exit_price'] / indic['entry_price'] - 1
    indic['mth_ret'].replace(-1, np.nan, inplace=True)

    print('adding features')

    indic = features.ma(indic)
    indic = features.yoy_ma(indic)
    indic = features.period_minmax(indic, indicator)
    indic = features.yoy_minmax(indic)

    # yoy to take in np.nan
    indic['yoy'] = indic['yoy'].replace(0, np.nan)

    # signal generation
    indic = signal(indic, constant.FEAT_COLS[0], 0, 0, 0)
    indic = signal(indic, constant.FEAT_COLS[1], 0, 0, 0)

    indic = signal(indic, constant.FEAT_COLS[2], 0.5, 0.5, 0.5)
    indic = signal(indic, constant.FEAT_COLS[3], 0.5, 0.5, 0.5)

    indic = signal(indic, constant.FEAT_COLS[4], 0.5, 0.5, 0.5)
    indic = signal(indic, constant.FEAT_COLS[5], 0, 0, 0)

    indic = signal(indic, constant.FEAT_COLS[6], 0.5, 0.5, 0.5)
    indic = signal(indic, constant.FEAT_COLS[7], 0, 0, 0)

    # signals for mean and median
    indic = signal_mm(indic, indicator, constant.FEAT_COLS[8], indic[indicator].mean(
    ), indic[indicator].mean(), indic[indicator].mean())
    indic = signal_mm(indic, indicator, constant.FEAT_COLS[9], indic[indicator].median(
    ), indic[indicator].median(), indic[indicator].median())

    # only for CHINA FED LOG***
    # if "PMI" in indicator:
    #     indic = signal_no_manipulation(indic, indicator, constant.FEAT_COLS[10], 51, 49)
    # else:
    #     pass

    # make a copy of dataframe
    df = indic.copy()

    # print(df.head())

    # iterate through constant.FEAT_COLS to get the strat
    for i in range(len(constant.FEAT_COLS)):
        df = f.my_strat(df, constant.FEAT_COLS[i])

    # get summary values
    values = [f.summary_values(df, indicator, constant.FEAT_COLS[i])
              for i in range(len(constant.FEAT_COLS))]
    
    # update dictionary
    results.update({indicator: dict(zip(constant.FEAT_COLS, values))})

    print('block ran in {0}s'.format(time.time() - startTime))

#     # create a folder to store plots
#     # folder_path = 'C:/Users/gerry.zeng/projects/plots/' + indicator
#     # access = 0o755

#     # try:
#     #     os.mkdir(folder_path, access)
#     # except OSError:
#     #     print("Creation of the directory %s failed" % folder_path)
#     # else:
#     #     print("Successfully created the directory %s" % folder_path)

#     # plots
#     # # No manipulation
#     # f.ticker_vs_com(df, indicator=indicator, comdty=comdty)

#     # # six charts
#     # f.ticker_mth_ret_plot(df, constant.FEAT_COLS[0], indicator=indicator)
#     # f.ticker_mth_ret_plot(df, constant.FEAT_COLS[1], indicator=indicator)
#     # f.ticker_mth_ret_plot(df, constant.FEAT_COLS[2], indicator=indicator)
#     # f.ticker_mth_ret_plot(df, constant.FEAT_COLS[3], indicator=indicator)

#     # f.arith_ret(df, constant.FEAT_COLS[0], indicator=indicator)
#     # f.arith_ret(df, constant.FEAT_COLS[1], indicator=indicator)
#     # f.arith_ret(df, constant.FEAT_COLS[2], indicator=indicator)
#     # f.arith_ret(df, constant.FEAT_COLS[3], indicator=indicator)

    # summary report
    df_list.append(f.summary_stats(df))
    overall_database.append(df)

    print('moving to next dataset')
    print(str(j) + ' iteration')

# unwragled results dataframe
results = f.results_viewer(results, 2)

# define file name
file_name = 's3_est_changed_release_dates'

# dump into pickle file
if __name__ == "__main__":
    # unwrangled results
    with open(constant.PARENT + file_name + '_results.pkl', 'wb') as fout:
        dump(results, fout)

    # summary report
    with open(constant.PARENT + file_name + '_summary_report.pkl', 'wb') as fout:
        dump(df_list, fout)

    # overall database on all the tickers
    with open(constant.PARENT + file_name + '_database.pkl', 'wb') as fout:
        dump(overall_database, fout)

    # store sharpe_ratio dataframe
    results.to_csv(constant.PARENT + file_name + '_sharpe_ratio.csv')
    print('code finished')
