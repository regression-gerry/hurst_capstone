'''
- rebuilding as classes
- make classes as functions to apply to dataframe
'''
import pandas as pd
import numpy as np
import logging


class Strategies():
    def __init__(self, df, logger=None):
        self.df = pd.DataFrame(df)
        self.logger = logger or logging.getLogger(__name__)

    # in development
    def minmax(self, col, no_period, min_period=None):
        '''
        minmax [minmax to ticker column]

        Parameters
        ----------
        col : [Series]
            [ticker as series]
        no_period : [int]
            [number of periods]
        min_period : [int], optional
            [minimum number of periods], by default None

        Returns
        -------
        [type]
            [description]
        '''
        num = col - col.rolling(no_period, min_period).min()
        deno = col.rolling(no_period, min_period).max() - \
            col.rolling(no_period, min_period).min()
        return num/deno

    def period_minmax(self, period, ticker_col_no, n, d):
        '''
        period_minmax [minmax on ticker]

        Parameters
        ----------
        period : [str]
            [period: 'daily', 'wk', 'mn']
        ticker_col_no : [int]
            [ticker column position]
        n : [int]
            [period to roll through]
        d : [int]
            [min period to roll through]

        Returns
        -------
        [df]
            [df]
        '''
        num = self.df.iloc[:, ticker_col_no] - \
            self.df.iloc[:, ticker_col_no].rolling(n, d).min()
        deno = self.df.iloc[:, ticker_col_no].rolling(
            n, d).max() - self.df.iloc[:, ticker_col_no].rolling(n, d).min()
        self.df[str(n) + period + '_minmax'] = num / deno
        return self.df

    def ma(self, ticker_col_no, n):
        '''
        ma [moving average function]

        Parameters
        ----------
        ticker_col_no : [int]
            [ticker column position]
        n : [int]
            [moving average periods]

        Returns
        -------
        [df]
            [df]
        '''
        a = self.df.iloc[:, ticker_col_no]
        b = self.df.iloc[:, ticker_col_no].rolling(n, n-2).mean()

        self.df[str(n) + 'wk_ma'] = (a/b) - 1
        return self.df

    def yoy_mn(self, ticker_col_no, date_col):
        '''
        yoy_mn [YoY change on monthly data]

        Parameters
        ----------
        df : [df]
            [df]
        ticker_col_no : [int]
            [ticker col position]
        date_col : [int]
            [date col position]

        Returns
        -------
        [df]
            [df]
        '''
        self.df['shifted_year'] = self.df[self.df.columns[date_col]
                                          ].dt.year.shift(-12)
        self.df['shifted_' + self.df.columns[ticker_col_no]
                ] = self.df[self.df.columns[ticker_col_no]].shift(-12)
        self.df['yoy'] = ((self.df['shifted_' + self.df.columns[ticker_col_no]] /
                           self.df[self.df.columns[ticker_col_no]]) - 1).shift(12)
        self.df = self.df.drop(
            ['shifted_year', 'shifted_' + self.df.columns[ticker_col_no]], \
            axis=1)
        return self.df

class Signal():
    def __init__(self, df, logger=None):
        self.df = pd.DataFrame(df)
        self.logger = logger or logging.getLogger(__name__)

    # def __init__(self):
    #     super(Signal, self).__init__()

    def strat_bins(self, feature):
        '''
        signal_bins [separates the sharpe ratio by bins]

        Parameters
        ----------
        df : [df]
            [df]
        feature : [str]
            [strings of feature names]

        Returns
        -------
        [df]
            [df]
        '''

        # bins = [0, 0.125, 0.5, 0.875, 1] # short bins to capture the extremities
        bins = [0, 0.25, 0.5, 0.75, 1]  # initial bins
        bin_no = ['1', '2', '3', '4']
        # values = ['1']

        conditions = [(self.df[feature] >= bins[0]) & (self.df[feature] < bins[1])]
        self.df['signal_' + feature + '_bins' + bin_no[0]] = np.select(conditions, '1')

        conditions = [(self.df[feature] >= bins[1]) & (self.df[feature] < bins[2])]
        self.df['signal_' + feature + '_bins' + bin_no[1]] = np.select(conditions, '1')

        conditions = [(self.df[feature] >= bins[2]) & (self.df[feature] < bins[3])]
        self.df['signal_' + feature + '_bins' + bin_no[2]] = np.select(conditions, '1')

        conditions = [(self.df[feature] >= bins[3]) & (self.df[feature] <= bins[4])]
        self.df['signal_' + feature + '_bins' + bin_no[3]] = np.select(conditions, '1')

        return self.df

    def signal_bins(self, feature, ret_col):
        '''
        my_strat_bins [summary]

        Parameters
        ----------
        df : [df]
            [existing dataframe]
        feature : [str]
            [minmax or feature names]
        ret_col : [str]
            [column name of returns]

        Returns
        -------
        [df]
            [make changes to the existing dataframe]
        '''
        long_bins = ['1', '2']
        short_bins = ['3', '4']

        for i in range(len(short_bins)):
            # df['signal_' + feature + '_bins' + bin_no[i]] = df['signal_' + feature + '_bins' + bin_no[i]].fillna(0)
            self.df['signal_' + feature + '_bins' + short_bins[i]] = self.df['signal_' +
                                                                   feature + '_bins' + short_bins[i]].fillna(0)
            self.df['signal_' + feature + '_bins' + long_bins[i]] = self.df['signal_' +
                                                                  feature + '_bins' + long_bins[i]].fillna(0)

            self.df['signal_' + feature + '_bins' + short_bins[i]] = self.df['signal_' +
                                                                   feature + '_bins' + short_bins[i]].astype('int32')
            self.df['signal_' + feature + '_bins' + long_bins[i]] = self.df['signal_' +
                                                                  feature + '_bins' + long_bins[i]].astype('int32')

            # LONG ON short BINS
            self.df['my_strat_' + feature + '_bins' + short_bins[i]] = self.df['signal_' +
                                                                     feature + '_bins' + short_bins[i]] * self.df[ret_col]

            self.df['my_strat_' + feature + '_bins' + long_bins[i]] = self.df['signal_' +
                                                                    feature + '_bins' + long_bins[i]] * self.df[ret_col]
        return self.df

