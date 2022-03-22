'''
- output signals for trades
'''
import pandas as pd
import numpy as np
import logging


class Signal():
    def __init__(self, df, logger=None):
        self.df = pd.DataFrame(df)
        self.logger = logger or logging.getLogger(__name__)
    
    def simple_signal(self, strategy, long, short):
        '''
        simple_signal [long or short as binaries]

        Parameters
        ----------
        strategy : [str]
            [strategy string]
        long : [int]
            [value for long condition]
        short : [int]
            [value for short]

        Returns
        -------
        [self.df]
            [self.df]
        '''
        # conditions of trade
        values = ['1', '-1', '0']
        # set conditions
        conditions = [
            (self.self.df[strategy] > long),
            (self.self.df[strategy] < short),
            (self.self.df[strategy] <= long) \
            & (self.self.df[strategy] >= short)
        ]
        self.self.df['signal_' + strategy] = np.select(conditions, values)
        return self.self.df

    def signal_ret(self, strategy):
        '''
        signal_ret [returns for signal]

        Parameters
        -------
        
        strategy : [str]
            [strings of strategy names]

        Returns
        -------
        [self.df]
            [self.df]
        '''
        self.df['signal_' + strategy] = self.df['signal_' + strategy].fillna(0)
        self.df['signal_' + strategy] = self.df['signal_' + strategy].astype('int32')
        self.df['strat_' + strategy] = self.df['signal_' + strategy] * self.df['ret']
        return self.df


    def singular_strat_bins(self, strategy, ret_col):
        '''
        singular_strat_bins [for singular test on a ticker]

        Parameters
        ----------
        strategy : [str]
            [column name of returns]
        ret_col : [str]
            [column name of returns]

        Returns
        -------
        [df]
            [make changes to the existing dataframe]
        '''
        short_bins = ['1', '2']
        long_bins = ['3', '4']

        bin_len = len(short_bins)

        for i in range(bin_len):
            # df['signal_' + strategy + '_bins' + bin_no[i]] = df['signal_' + strategy + '_bins' + bin_no[i]].fillna(0)
            self.df['signal_' + strategy + '_bins' + short_bins[i]] = self.df['signal_' +
                                                                strategy + '_bins' + short_bins[i]].astype('int32')
            self.df['signal_' + strategy + '_bins' + long_bins[i]] = self.df['signal_' +
                                                                strategy + '_bins' + long_bins[i]].astype('int32')

            self.df['my_strat_' + strategy + ret_col + '_bins' + short_bins[i]] = -1 * self.df['signal_' +
                                                                                    strategy + '_bins' + short_bins[i]] * self.df[ret_col]

            self.df['my_strat_' + strategy + ret_col + '_bins' + long_bins[i]] = self.df['signal_' +
                                                                            strategy + '_bins' + long_bins[i]] * self.df[ret_col]
        return self.df