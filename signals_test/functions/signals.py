import numpy as np
import pandas as pd

'''
Signal generation
'''
# signal


def signal(df, feature, c1, c2, c3):
    '''
    signal [function to create signal column]

    Parameters
    ----------
    df : [DataFrame]
        [existing df]
    feature : [str]
        [str type from features list]
    c1 : [int]
        [Long trade]
    c2 : [int]
        [Short trade]
    c3 : [int]
        [No trade]

    Returns
    -------
    [DataFrame]
        [creates and adds the signal column to the existing DataFrame]
    '''
    # correlation of monthly return against the feature
    # cor = df['mth_ret'].corr(df[feature])

    # changed on 20/1 to account for the '-' SR
    ### disregard correlation 
    values = ['1', '-1', '0']
    conditions = [
            (df[feature] > c1),
            (df[feature] < c2),
            (df[feature] == c3)
        ]
    df['signal_' + feature] = np.select(conditions, values)


    # conditions of trade
    # values = ['1', '-1', '0']
    # if cor > 0:
    #     # set the conditions
    #     conditions = [
    #         (df[feature] > c1),
    #         (df[feature] < c2),
    #         (df[feature] == c3)
    #     ]
    #     df['signal_' + feature] = np.select(conditions, values)
    # else:
    #     conditions = [
    #         (df[feature] < c1),
    #         (df[feature] > c2),
    #         (df[feature] == c3)
    #     ]
    #     df['signal_' + feature] = np.select(conditions, values)
    return df

# no manipulations
def signal_mm(df, indicator, feature, c1, c2, c3):
    '''
    signal_none [summary]

    Parameters
    ----------
    df : [type]
        [description]
    indicator : [type]
        [description]
    feature : [none - no manipulation]
        [description]
    c1 : [type]
        [description]
    c2 : [type]
        [description]
    c3 : [type]
        [description]

    Returns
    -------
    [type]
        [description]
    '''
    # conditions of trade
    values = ['1', '-1', '0']

    # set conditions
    conditions = [
        (df[indicator] > c1),
        (df[indicator] < c2),
        (df[indicator] == c3)
    ]

    df['signal_' + feature] = np.select(conditions, values)
    return df

def signal_no_manipulation(df, indicator, feature, c1, c2):
    '''
    signal_none [summary]

    Parameters
    ----------
    df : [type]
        [description]
    indicator : [type]
        [description]
    feature : [none - no manipulation]
        [description]
    c1 : [type]
        [description]
    c2 : [type]
        [description]
    c3 : [type]
        [description]

    Returns
    -------
    [type]
        [description]
    '''
    # conditions of trade
    values = ['1', '-1', '0']

    # set conditions
    conditions = [
        (df[indicator] > c1),
        (df[indicator] < c2),
        (df[indicator] <= c1) & (df[indicator] >= c2)
    ]

    df['signal_' + feature] = np.select(conditions, values)
    return df



