import pandas as pd
import numpy as np
import logging

class Strategies():
    def __init__(self, df, logger=None):
        self.df = pd.DataFrame(df)
        self.logger = logger or logging.getLogger(__name__)

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