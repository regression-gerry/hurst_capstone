'''
    [plotting script for each ticker in summary reports]
'''
# libraries for plotting
import os
import pandas as pd
import constant
import func as f
import matplotlib.pyplot as plt
import seaborn as sns
import sys
import time
sns.set_theme(style="darkgrid")

startTime = time.time()

# set path for scripts
sys.path.insert(1, '../data')

# read overall database for plotting
# overall_database = pd.read_pickle(constant.STATS_PATH_SOY[2])
overall_database = pd.read_pickle('./data/s3_projected_new_2000_database.pkl')

### name the tickers list with its inital index
tickers_list = pd.read_pickle('./data/s3_projected_new_2000_database.pkl')
# tickers_list = tickers_list[tickers_list['ticker'].str.contains(
#     'Index')].iloc[:, 0].drop_duplicates().reset_index(drop=True)
# tickers_list = tickers_list.reset_index()

tickers_list = [tickers_list[i].columns[2] for i in range(len(tickers_list))]
tickers_list = pd.DataFrame({'ticker': tickers_list})

# tickers_list = tickers_list['ticker']
tickers_list = tickers_list.reset_index()

# define indicator and comdty string
com = pd.read_csv('./data/soy_daily_returns.csv', usecols=[0, 2])
com.date = pd.to_datetime(com.date, format='%d/%m/%Y')
comdty = com.columns[1]  # comdty str

# read stats file to get the best feature
stats = pd.read_csv('./data/s3_projected_new_2000_stats.csv', index_col=False)

stats = stats[['ticker', 'best_feature']]

tickers_list = pd.concat([tickers_list.set_index('ticker'),
                          stats.set_index('ticker')], axis=1, join='inner')

# tickers_list = pd.concat([tickers_list,
#                           stats.set_index('ticker')], axis=1, join='inner')

tickers_list = tickers_list.reset_index()
tickers_list = tickers_list.drop(columns='index')

for i in range(len(tickers_list)):

    unwanted_features = ['mean', 'median']

    if tickers_list.best_feature[i] in unwanted_features:
        continue
    else:
        # create folder to store plots
        folder_path = 'C:/Users/gerry.zeng/projects/plots/' + \
            overall_database[i].columns[1]
        access = 0o755

        try:
            os.mkdir(folder_path, access)
        except OSError:
            print("Creation of the directory %s failed" % folder_path)
        else:
            print("Successfully created the directory %s" % folder_path)

        f.ticker_vs_com(
            overall_database[i], indicator=overall_database[i].columns[1], comdty=comdty)

        f.ticker_mth_ret_plot(
            overall_database[i], tickers_list.best_feature[i], indicator=overall_database[i].columns[1])

        f.arith_ret(overall_database[i], tickers_list.best_feature[i],
                    indicator=overall_database[i].columns[1])

        f.ticker_and_com(
            overall_database[i], indicator=overall_database[i].columns[1], comdty=comdty)

        print('block ran in {0}s'.format(time.time() - startTime))
        print(str(i) + ' iteration')
        print('moving to next ticker')

print('entire block ran in {0}s'.format(time.time() - startTime))
