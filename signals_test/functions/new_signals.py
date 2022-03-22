# libraries
import numpy as np
import pandas as pd


# --------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------
MONTHS = [12, 24, 36, 60]

MONTHS_D = [10, 20, 30, 50]

WEEKS = [52, 104, 156, 260]

WEEKS_D = [42, 84, 120, 200]

MN_MINMAX_FEATURES = [
    'yoy_12mn_minmax', '12mn_minmax',
    '24mn_minmax', '36mn_minmax',
    '60mn_minmax'
]

WK_MINMAX_FEATURES = [
    'yoy_52wk_minmax', '52wk_minmax',
    '104wk_minmax', '156wk_minmax',
    '260wk_minmax'
]


def comdty_parser(folder, file_name, cols):
    '''
    comdty_parser [Get the returns dataframe]

    Parameters
    ----------
    cols : [list]
        [list of column position in returns file]

    Returns
    -------
    [df]
        [comdty returns dataframe]
    '''
    ret = pd.read_csv('./' + folder + '/' + file_name + '.csv',
                      usecols=cols)
    ret.date = pd.to_datetime(ret.date, format='%d/%m/%Y')
    ret.set_index('date', inplace=True)
    return ret

# In progress
# def comdty_wrangle(df):
#     '''
#     comdty_wrangle [summary]

#     Parameters
#     ----------
#     df : [df]
#         [parse in comdty; specifically base metals]

#     Returns
#     -------
#     [df]
#         [ticker with its associated returns]
#     '''
#     df - df.


# --------------------------------------------------------------------
# Features
# --------------------------------------------------------------------
def ma(df, n):
    '''
    ma [summary]

    Parameters
    ----------
    df : [df]
        [df]
    n : [int]
        [no. of months]

    Returns
    -------
    [df]
        [df]
    '''
    a = df.iloc[:, 2]
    b = df.iloc[:, 2].rolling(n, n-2).mean()

    df[str(n) + 'mn_ma'] = (a/b) - 1
    return df


def period_minmax(df, p, col_no, n, d):
    '''
    period_minmax [minmax on the dataframe]

    Parameters
    ----------
    df : [df]
        [df]
    p : [str]
        [period: 'wk', 'mn']
    col_no : [int]
        [specifies the column position for proj or est data]
    n : [int]
        [period to roll through]
    d : [int]
        [min period to roll through]

    Returns
    -------
    [df]
        [df]
    '''
    num = df.iloc[:, col_no] - df.iloc[:, col_no].rolling(n, d).min()
    deno = df.iloc[:, col_no].rolling(
        n, d).max() - df.iloc[:, col_no].rolling(n, d).min()
    # deno = df.iloc[:, col_no].rolling(n, d).apply(lambda x: max(x) - min(x))
    df[str(n) + p + '_minmax'] = num/deno
    return df


def yoy_mn(df, col_no, date_col):
    '''
    yoy_mn [YoY change on Monthly data]

    Parameters
    ----------
    df : [df]
        [df]
    col_no : [int]
        [column position for proj or est or ticker data]

    Returns
    -------
    [df]
        [df]
    '''
    df['shifted_year'] = df[df.columns[date_col]].dt.year.shift(-12)
    df['shifted_' + df.columns[col_no]] = df[df.columns[col_no]].shift(-12)
    df['yoy'] = ((df['shifted_' + df.columns[col_no]] /
                  df[df.columns[col_no]]) - 1).shift(12)
    df = df.drop(
        ['shifted_year', 'shifted_' + df.columns[col_no]], axis=1)
    return df


def yoy_minmax(df, p, n, d):
    '''
    yoy_minmax [minmax on yoy data]

    Parameters
    ----------
    df : [df]
        [df]
    p : [str]
        ['wk' or 'mn']
    n : [int]
        [period to roll]
    d : [int]
        [min period]

    Returns
    -------
    [df]
        [df]
    '''
    num = (df.yoy - df.yoy.rolling(n, d).min())
    deno = (df.yoy.rolling(n, d).max() - df.yoy.rolling(n, d).min())
    # deno = df.yoy.rolling(n, d).apply(lambda x: max(x) - min(x))
    df['yoy_' + str(n) + p + '_minmax'] = num/deno
    return df

# -------------------------------------------------------------------------
# Signals
# -------------------------------------------------------------------------


def signal_no_changes(df, feature, c1, c2):
    '''
    signal_no_changes []

    Parameters
    ----------
    df : [df]
        [df]
    feature : [str]
        [strings of feature]
    c1 : [int]
        [value for long condition]
    c2 : [int]
        [value for short]

    Returns
    -------
    [df]
        [df]
    '''
    # conditions of trade
    values = ['1', '-1', '0']
    # if minmax > 0.5 or ma > 0
    # set conditions
    conditions = [
        (df[feature] > c1),
        (df[feature] < c2),
        (df[feature] <= c1) & (df[feature] >= c2)
    ]
    df['signal_' + feature] = np.select(conditions, values)
    return df


def my_strat(df, feature):
    '''
    my_strat [multiply the binary integers to returns associated with it]

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
    df['signal_' + feature] = df['signal_' + feature].fillna(0)
    df['signal_' + feature] = df['signal_' + feature].astype('int32')
    df['my_strat_' + feature] = df['signal_' + feature] * df['ret']
    return df

# on inital bins


def signal_bins(df, feature):
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

    conditions = [(df[feature] >= bins[0]) & (df[feature] < bins[1])]
    df['signal_' + feature + '_bins' + bin_no[0]] = np.select(conditions, '1')

    conditions = [(df[feature] >= bins[1]) & (df[feature] < bins[2])]
    df['signal_' + feature + '_bins' + bin_no[1]] = np.select(conditions, '1')

    conditions = [(df[feature] >= bins[2]) & (df[feature] < bins[3])]
    df['signal_' + feature + '_bins' + bin_no[2]] = np.select(conditions, '1')

    conditions = [(df[feature] >= bins[3]) & (df[feature] <= bins[4])]
    df['signal_' + feature + '_bins' + bin_no[3]] = np.select(conditions, '1')

    return df


# CHANGES -> fillna(0) on upsampling to daily_ret
def my_strat_bins(df, feature, ret_col):
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
    short_bins = ['1', '2']
    long_bins = ['3', '4']

    for i in range(len(short_bins)):
        # df['signal_' + feature + '_bins' + bin_no[i]] = df['signal_' + feature + '_bins' + bin_no[i]].fillna(0)
        df['signal_' + feature + '_bins' + short_bins[i]] = df['signal_' +
                                                               feature + '_bins' + short_bins[i]].fillna(0)
        df['signal_' + feature + '_bins' + long_bins[i]] = df['signal_' +
                                                              feature + '_bins' + long_bins[i]].fillna(0)

        df['signal_' + feature + '_bins' + short_bins[i]] = df['signal_' +
                                                               feature + '_bins' + short_bins[i]].astype('int32')
        df['signal_' + feature + '_bins' + long_bins[i]] = df['signal_' +
                                                              feature + '_bins' + long_bins[i]].astype('int32')

        # LONG ON ALL BINS
        df['my_strat_' + feature + '_bins' + short_bins[i]] = df['signal_' +
                                                                 feature + '_bins' + short_bins[i]] * df[ret_col]

        df['my_strat_' + feature + '_bins' + long_bins[i]] = df['signal_' +
                                                                feature + '_bins' + long_bins[i]] * df[ret_col]
    return df


def singular_strat_bins(df, feature, ret_col):
    '''
    singular_strat_bins [for singular test on a ticker]

    Parameters
    ----------
    df : [df]
        [existing dataframe]
    feature : [str]
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

    for i in range(len(short_bins)):
        # df['signal_' + feature + '_bins' + bin_no[i]] = df['signal_' + feature + '_bins' + bin_no[i]].fillna(0)
        df['signal_' + feature + '_bins' + short_bins[i]] = df['signal_' +
                                                               feature + '_bins' + short_bins[i]].astype('int32')
        df['signal_' + feature + '_bins' + long_bins[i]] = df['signal_' +
                                                              feature + '_bins' + long_bins[i]].astype('int32')

        df['my_strat_' + feature + ret_col + '_bins' + short_bins[i]] = -1 * df['signal_' +
                                                                                feature + '_bins' + short_bins[i]] * df[ret_col]

        df['my_strat_' + feature + ret_col + '_bins' + long_bins[i]] = df['signal_' +
                                                                          feature + '_bins' + long_bins[i]] * df[ret_col]
    return df

##### Work in progress
def monthly_data_wrangle(df, ticker_col, date_col, data_source):
    '''
    monthly_data_wrangle [
        wrangle function to be placed in main script
        ]

    Parameters
    ----------
    df : [dataframe]
        [monthly ticker dataframe]
    ticker_col : [int]
        [column of ticker value]
    date_col : [int]
        [date column]
    data_source : [str]
        [string of data source type]
    ret_df : [dataframe]
        [daily returns dataframe]
    ret_col : [int]
        [column no for daily returns of comdty, usually at 0]

    Returns
    -------
    [dataframe]
        [wrangled dataframe being combined]
    '''

    # resample columns
    df = df.reset_index(drop=True)
    df = df.set_index(df.columns[date_col]).resample(
        'M').asfreq().fillna(np.nan).reset_index()
    if data_source == 'wasde':
        # replace the day with 12th of the month instead
        df[df.columns[date_col]] = df[df.columns[date_col]].apply(
            lambda dt: dt.replace(day=12))
    else:
        pass

    df = df.replace(0, np.nan)  # for yoy calculation
    df['entry_date'] = df[df.columns[date_col]] + \
        BDay(1)  # trade the following day

    # features
    df = yoy_mn(df, ticker_col)  # do a yoy
    df = yoy_minmax(df, 'mn', MONTHS[0], MONTHS_D[0])
    for m_i, m_j in zip(MONTHS, MONTHS_D):
        df = period_minmax(df, 'mn', ticker_col, m_i, m_j)

    # create signal bins and columns
    for i in range(len(MN_MINMAX_FEATURES)):
        df = signal_bins(df, MN_MINMAX_FEATURES[i])

    # to concat with daily returns
    df.set_index(df['entry_date'], inplace=True)


def monthly_data_join(df, ret_df, ret_col):
    '''
    monthly_data_join [
        join wrangled monthly data with daily returns and
        add binning of signal columns
    ]

    Parameters
    ----------
    df : [df]
        [wrangled monthly data]
    ret_df : [df]
        [daily returns]
    ret_col : [int]
        [column position for daily returns in combined df]

    Returns
    -------
    [df]
        [description]
    '''    
    df = pd.concat([ret_df, df], axis=1).reindex(ret_df.index)
    df.iloc[:, 0:len(ret_df)] = df.iloc[:, 0:len(ret_df)]/100

    # find columns whose name contains specific strings
    df.loc[:, df.columns.str.contains(
        "bins")] = df.loc[:, df.columns.str.contains("bins")].fillna(method='ffill')
    # shift the bins columns by 1 to align with returns
    df.loc[:, df.columns.str.contains(
        "bins")] = df.loc[:, df.columns.str.contains("bins")].shift(1)

    # choose the correct comdty returns
    for i in range(len(MN_MINMAX_FEATURES)):
        # get the signals
        df = my_strat_bins(
            df, MN_MINMAX_FEATURES[i], df.columns[ret_col])
    return df


def weekly_data_wrangle(df, ticker_col, date_col, ret_df, ret_col):
    df = df.set_index(df.columns[0]).resample(
        'W-{:%a}'.format(df.date[0])).asfreq().fillna(np.nan).reset_index()

    df['entry_date'] = df[df.columns[date_col]] + BDay(1)
    df['week'] = df[df.columns[date_col]].dt.week
    df.replace({'week': 53}, 52, inplace=True)

    # features
    df['yoy'] = df.groupby(['week'])[df.columns[ticker_col]].pct_change()
    df = df.drop(['week'], axis=1)
    df = yoy_minmax(df, 'wk', WEEKS[0], WEEKS_D[0])
    for m_i, m_j in zip(WEEKS, WEEKS_D):
        df = period_minmax(df, 'wk', ticker_col, m_i, m_j)

    # create signal bins and columns
    for i in range(len(WK_MINMAX_FEATURES)):
        df = signal_bins(df, WK_MINMAX_FEATURES[i])

    # to concat with daily returns
    df.set_index(df['entry_date'], inplace=True)
