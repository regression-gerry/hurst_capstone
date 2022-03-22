import pandas as pd
import numpy as np
from pandas.tseries.offsets import DateOffset, BDay
from data_updater.models.fundamental_schema import *
from data_updater.models.writer import *
from signals.dev.class_test import *
from data_updater.dao.fundamental_time_series_dao import FundamentalTimeSeriesDao
from datetime import datetime, timedelta
import time

params = {
    'url_local': 'postgresql://postgres:123@SGMAR-INVLT983H:5432/fun_test',
    'url_jb': 'postgresql://postgres:1@INV_Jianbin:5432/fundamental_jb'
}

# if __name__ == "main":

db_connector = FundamentalTimeSeriesDao(url=params['url_local'])

# db data
data = db_connector.get_last_n_fundamental_time_series(identifier_list=['NOAANT34 ANOM Index'],
                                                       data_source_id_list=[2])

# db data wrangle
data = data[['identifier', 'report_date', 'release_date', 'field_value']]
data.sort_values(by = ['report_date'], inplace=True)
data.iloc[:, 1:3] = data.iloc[:, 1:3].apply(pd.to_datetime)
data['entry'] = data['release_date'] + BDay(1)
data = data[data['entry'] < '2021-01-04']
data.set_index('entry', drop=False, inplace=True)

## comd ret ref + wrangle
# change the path to data_updater/data/comd_ret.csv to retrieve comdty returns
ret = pd.read_csv('C:/Users/gerry.zeng/qf2_projects/data_updater/data/comd_ret.csv', usecols = [0, 48])
ret.rename(columns={ret.columns[0]: 'date'}, inplace=True)
ret['date'] = pd.to_datetime(ret['date'], format='%d/%m/%Y')
ret.set_index('date', inplace=True)

# set arbitrary ret column
data[ret.columns[0] + '_ret'] = np.random.randint(100)

# combine ret with db data
combined_data = pd.concat([ret, data], axis=1).reindex(ret.index)

# set date ranges to calculate returns
starting_dates = data.loc[data.index == data.entry].index + BDay(1)
# starting_dates = starting_dates
ending_dates = data.loc[data.index == data.entry].index[1:]
ending_dates = ending_dates.append(ret.index[-1:])

# get all returns between starting and ending dates
all_ret = [np.sum(combined_data.loc[(combined_data.index >= starting_dates[i]) & (
                combined_data.index <= ending_dates[i])][combined_data.columns[0]])
           for i in range(len(starting_dates))]

# setting the arbitrarily-filled placeholders to be the actual returns
combined_data.loc[~combined_data[combined_data.columns[0] + '_ret'].isnull(), [combined_data.columns[0] + '_ret']] = all_ret

# re-wrangle the combined dataframe
combined_data = combined_data.loc[~combined_data[combined_data.columns[0] + '_ret'].isnull()]

# reset the returns
combined_data[combined_data.columns[0] + '_ret'] = combined_data[combined_data.columns[0] + '_ret'] / 100
combined_data = combined_data.reset_index(drop=True)
combined_data.drop([ret.columns[0]], axis=1, inplace=True)

# print(combined_data.columns)

# initialising combined_data into Strat obj
strat = Strategies(combined_data)

# 3mn moving avg
strat_data = strat.ma(ticker_col_no=3, n=14) # 14 weeks ~ 3months
strat_data = Signal(strat_data).strat_bins(feature=combined_data.columns[-1])
# print(combined_data.columns)
strat_data = Signal(strat_data).signal_bins(feature=combined_data.columns[-1], ret_col=combined_data.columns[-2])

print('Cumulative ret for long bin 1: {}'.format(np.sum(strat_data[strat_data.columns[-2]])))
print('Cumulative ret for long bin 2: {}'.format(np.sum(strat_data[strat_data.columns[-1]])))