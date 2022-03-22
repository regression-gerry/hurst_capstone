import numpy as np
import pandas as pd

'''
Functions for feature columns
'''
def ma(df):
    '''
    ma [summary]

    Parameters
    ----------
    df : [type]
        [description]

    Returns
    -------
    [type]
        [description]
    '''
    # position of indicator changed for new daily returns
    a = df.iloc[:, 2]
    b = df.iloc[:, 2].rolling(6).mean()
    c = df.iloc[:, 2].rolling(12, 10).mean()

    df['6mn_ma'] = a - b

    df['12mn_ma'] = a - c
    return df


# yoy + ma
def yoy_ma(df):
    '''
    yoy_ma [summary]

    Parameters
    ----------
    df : [type]
        [description]

    Returns
    -------
    [type]
        [description]
    '''
    # changed for daily returns
    df['yoy'] = df.groupby([df.iloc[:, 1].dt.month])[
        str(df.columns[2])].pct_change()

    df['yoy_6mn_ma'] = df.yoy - df.yoy.rolling(6).mean()
    df['yoy_12mn_ma'] = df.yoy - df.yoy.rolling(12, 10).mean()
    return df


# oscillator based on the ticker's value
def period_minmax(df, indicator):
    '''
    period_minmax [summary]

    Parameters
    ----------
    df : [type]
        [description]
    indicator : [type]
        [description]

    Returns
    -------
    [type]
        [description]
    '''
    num = df[indicator] - df[indicator].rolling(12, 10).min()
    # deno =  df.yoy.rolling(12, min_periods=1).max() - df.yoy.rolling(12, min_periods=1).min()
    deno = df[indicator].rolling(12, 10).apply(lambda x: max(x) - min(x))
    df['12mn_minmax'] = num/deno

    num1 = df[indicator] - df[indicator].rolling(6).min()
    # deno =  df.yoy.rolling(12, min_periods=1).max() - df.yoy.rolling(12, min_periods=1).min()
    deno1 = df[indicator].rolling(6).apply(lambda x: max(x) - min(x))
    df['6mn_minmax'] = num1/deno1

    return df


# yoy + minmax
def yoy_minmax(df):
    '''
    yoy_minmax [summary]

    Parameters
    ----------
    df : [type]
        [description]

    Returns
    -------
    [type]
        [description]
    '''
    # changed for daily returns
    df['yoy'] = df.groupby([df.iloc[:, 1].dt.month])[
        str(df.columns[2])].pct_change()
    
    num = df.yoy - df.yoy.rolling(12, 10).min()
    deno = df.yoy.rolling(12, 10).apply(lambda x: max(x) - min(x))
    df['yoy_12mn_minmax'] = round(num/deno, 3)

    # for 6 mn ma
    num1 = df.yoy - df.yoy.rolling(6).min()
    deno1 = df.yoy.rolling(6).apply(lambda x: max(x) - min(x))
    df['yoy_6mn_minmax'] = round(num1/deno1, 3)
    
    return df
