'''
[This script contains all functions]
'''
# import constant
from datetime import datetime
import numpy as np
import pandas as pd
# import pickle
from pandas.tseries.offsets import BDay
import math
import matplotlib.pyplot as plt
import seaborn as sns
sns.set_theme(style="darkgrid")

#### deprecated ####
# for tickers with release dates


def release_dates(df):
    '''
    release_dates [summary]

    Parameters
    ----------
    df : [type]
        [description]

    Returns
    -------
    [type]
        [description]
    '''
    dates_list = list(df['period_end'].apply(
        lambda x: x.strftime('%Y%m%d')))

    release_dates = [blp.bdp([  indicator], flds=[
        "ECO_FUTURE_RELEASE_DATE"], start_dt=dates_list[i]) for i in range(len(dates_list))]

    # if there are no release dates
    if len(release_dates) == 0:
        df['trading_date'] = df['period_end'] + pd.DateOffset(days=15)
        df['trading_date'] = np.where(df.trading_date.dt.day_name().isin(
            ['Saturday', 'Sunday']), df.trading_date + BDay(1), df.trading_date)
    else:
        # append release dates
        df['release_date'] = pd.Series(
            [release_dates[i].values for i in range(len(release_dates))])
        for _ in range(2):
            df['release_date'] = df['release_date'].str.get(0)
        df['release_date'] = pd.to_datetime(df['release_date']).dt.date
    return df


# tickers with no release dates
def release_date_test(df, d):
    '''
    release_date_test [creates release dates and entry dates columns]

    Parameters
    ----------
    df : [DataFrame]
        [initial dataframe]
    d : [int]
        [no. of days to add for release dates]

    Returns
    -------
    [DataFrame]
        [updated df]
    '''
    df['release_date_test'] = df[df.columns[0]] + pd.DateOffset(days=d)
    df['entry_date'] = df['release_date_test'] + BDay(1)
    return df
##############################################################################

# entry price
def get_entry_price(df, mapping):
    '''
    get_entry_price [obtain copper prices for entry date]

    Parameters
    ----------
    df : [DataFrame]
        [inital dataframe]
    mapping : [dict]
        [dict of copper values and dates]

    Returns
    -------
    [DataFrame]
        [updated dataframe]
    '''

    df['entry_price'] = df['entry_date'].map(mapping)
    if df.entry_price.isnull:
        for _ in range(6):
            df.loc[df['entry_price'].isnull(
            ), 'entry_date'] = df.loc[df['entry_price'].isnull(), 'entry_date'] + BDay(1)
            df['entry_price'] = df['entry_date'].map(mapping)
    else:
        pass
    return df


# exit price
def get_exit_price(df, mapping, n):
    '''
    get_exit_price [obtain copper prices for exit dates]

    Parameters
    ----------
    df : [type]
        [description]
    mapping : [type]
        [description]
    n : [type]
        [description]

    Returns
    -------
    [type]
        [description]
    '''
    # exit date is one month after release date
    df['exit_date'] = df[df.columns[2]] + pd.DateOffset(months=n) + BDay(1)

    df[['entry_date', 'exit_date']] = df[[
        'entry_date', 'exit_date']].apply(pd.to_datetime)

    # prevent overlap except for last row
    if df[df.columns[2]].isnull().values.any():
        diff = df['exit_date'].iloc[:-1] - df['entry_date'].shift(-1)
        diff = diff > pd.Timedelta(0)
        if len(df['exit_date'].iloc[:-1].dropna()[diff]) > 0:
            df['exit_date'].iloc[:-1] = df['entry_date'].shift(-1).dropna()
            # df['exit_date_test'] = df['exit_date'].iloc[:-1].dropna()[diff]
        else:
            pass
    else:
        diff = df['exit_date'].iloc[:-1] > df['entry_date'].shift(-1).dropna()
        if not df['exit_date'].iloc[:-1][diff].empty:
            df['exit_date'].iloc[:-1] = df['entry_date'].shift(-1).dropna()
        else:
            pass

    # map copper prices
    df['exit_price'] = df['exit_date'].map(mapping)

    # iterate 6 times to look for comdty price
    for _ in range(6):
        df.loc[df['exit_price'].isnull(
        ), 'exit_date'] = df.loc[df['exit_price'].isnull(), 'exit_date'] + BDay(1)
        df['exit_price'] = df['exit_date'].map(mapping)

    # any 0 values be empty
    df['exit_price'].replace(0, np.nan, inplace=True)

    return df


'''
Functions for feature columns
'''


def ma(df):
    '''
    ma [moving average feature; 6 and 12 month]

    Parameters
    ----------
    df : [type]
        [description]

    Returns
    -------
    [type]
        [description]
    '''
    a = df.iloc[:, 1]
    b = df.iloc[:, 1].rolling(6, 4).mean()

    c = df.iloc[:, 1]
    d = df.iloc[:, 1].rolling(12, 10).mean()

    df['6mn_ma'] = (a/b) - 1

    df['12mn_ma'] = (c/d) - 1
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
    df['yoy'] = df.groupby([df.iloc[:, 0].dt.month])[
        df.columns[1]].pct_change()

    df['yoy_6mn_ma'] = df.yoy - df.yoy.rolling(6, 4).mean()
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

    num1 = df[indicator] - df[indicator].rolling(6, 4).min()
    # deno =  df.yoy.rolling(12, min_periods=1).max() - df.yoy.rolling(12, min_periods=1).min()
    deno1 = df[indicator].rolling(6, 4).apply(lambda x: max(x) - min(x))
    df['6mn_minmax'] = num1/deno1

    return df


# yoy + minmax
def yoy_minmax(df):
    '''
    yoy_minmax [add 6 or 12 months minmax to YoY]

    Parameters
    ----------
    df : [DataFrame]
        [existing dataframe]

    Returns
    -------
    [DataFrame]
        [adds two columns; 'yoy_12mn_minmax', 'yoy_6mn_minmax']
    '''
    # run a yoy
    df['yoy'] = df.groupby([df.iloc[:, 0].dt.month])[
        df.columns[1]].pct_change()

    # for 12mn minmax
    num = df.yoy - df.yoy.rolling(12, 10).min()
    deno = df.yoy.rolling(12, 10).apply(lambda x: max(x) - min(x))
    df['yoy_12mn_minmax'] = round(num/deno, 3)

    # for 6 mn minmax
    num1 = df.yoy - df.yoy.rolling(6, 4).min()
    deno1 = df.yoy.rolling(6, 4).apply(lambda x: max(x) - min(x))
    df['yoy_6mn_minmax'] = round(num1/deno1, 3)

    return df


'''
Signal generation
'''
# signal


def signal(df, feature, c1, c2, c3):
    '''
    signal [summary]

    Parameters
    ----------
    df : [type]
        [description]
    feature : [type]
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
    cor = df['mth_ret'].corr(df[feature])
    values = ['1', '-1', '0']
    if cor > 0:
        # set the conditions
        conditions = [
            (df[feature] > c1),
            (df[feature] < c2),
            (df[feature] == c3)
        ]
        df['signal_' + feature] = np.select(conditions, values)
    else:
        conditions = [
            (df[feature] < c1),
            (df[feature] > c2),
            (df[feature] == c3)
        ]
        df['signal_' + feature] = np.select(conditions, values)
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

#### new signal generation (one hot encoding with nan)
### work in progress
# def new_signal(df, feature): 
#     '''
#     new_signal [summary]

#     Parameters
#     ----------
#     df : [type]
#         [description]
#     feature : [type]
#         [description]
#     '''

#     bins = [0, 0.25, 0.5, 0.75, 1]
#     bin_no = ['1', '2', '3', '4']

#     for i in range(len(bins)):
#         for j in range(len(bin_no)):
#         values = [1, np.nan]

#         conditions = [
#             (df[feature] >= bins[i]),
#             (df[feature] < bins[i+1])
#         ]
#         if bin

#         df['signal_' + feature + bin_no[j]] = np.select(conditions, values)

#     df['signal_' + feature + '_bin1'] = 
#     df['signal_' + feature + '_bin2']
#     df['signal_' + feature + '_bin3']
#     df['signal_' + feature + '_bin4']

# Results
def results_viewer(dic, n):
    '''
    results_viewer [summary]

    Parameters
    ----------
    dic : [type]
        [description]
    n : [type]
        [description]

    Returns
    -------
    [type]
        [description]
    '''
    results = pd.DataFrame.from_dict(dic, orient='index')

    # separate the no manipulation
    results_none = results.iloc[:, -n:].reset_index()
    results_none.rename(columns={'index': 'ticker'}, inplace=True)
    results_none.ticker += ' sharpe ratio'
    results_none.set_index('ticker', inplace=True)

    # on results
    results = results.iloc[:, :-n]
    results = results.apply(lambda x: x.explode(
    ) if x.name in results.columns else x).reset_index().rename(columns={'index': 'ticker'})
    # results.loc[0::2, "ticker"] += ' correlation'
    results.loc[1::2, "ticker"] += ' sharpe ratio'

    # results_cor = results[results['ticker'].str.contains("cor")]
    # results_cor.set_index('ticker', inplace=True)
    # results_cor.insert(0, "max_values", results_cor.max(1))
    # results_cor.sort_values("max_values", ascending=False, inplace=True)

    results_sr = results[results['ticker'].str.contains("sharpe ratio")]
    results_sr.set_index('ticker', inplace=True)
    results_sr = pd.concat([results_sr, results_none], axis=1)
    results_sr.insert(0, "sharpe_ratio", results_sr.max(1))
    results_sr.sort_values("sharpe_ratio", ascending=False, inplace=True)

    return results_sr


'''
Plotting functions
'''

# define path to save plots to
save_results_to = 'C:/Users/gerry.zeng/projects/plots/'


def ticker_mth_ret_plot(df, feature, indicator):
    '''
    ticker_mth_ret_plot [summary]

    Parameters
    ----------
    df : [type]
        [description]
    feature : [type]
        [description]
    indicator : [type]
        [description]

    Returns
    -------
    [type]
        [description]
    '''
    df = df.set_index('entry_date')
    fig = sns.lmplot(x=feature, y='mth_ret', data=df,
                     line_kws={'color': 'red'})
    plt.ylabel('1-month return of LP3')
    plt.title(feature + ' vs 1-month return of LP3')
    # plt.show()
    fig.savefig(save_results_to + '/' + indicator + '/' + feature +
                ' vs 1-month return of LP3.png', dpi=300)
    plt.close('all')


def ticker_vs_com(df, indicator, comdty):
    '''
    ticker_vs_com [summary]

    Parameters
    ----------
    df : [type]
        [description]
    indicator : [type]
        [description]
    comdty : [type]
        [description]
    '''
    df = df.set_index('entry_date')
    fig = sns.lmplot(x=indicator, y='entry_price',
                     data=df, line_kws={'color': 'red'})
    plt.ylabel(comdty)
    plt.title(indicator + ' vs ' + comdty)
    # plt.show()
    fig.savefig(save_results_to + indicator + '/' + indicator +
                ' vs ' + comdty + '.png', dpi=300)
    plt.close('all')


def my_strat(df, feature):
    '''
    my_strat [summary]

    Parameters
    ----------
    df : [type]
        [description]
    feature : [type]
        [description]

    Returns
    -------
    [type]
        [description]
    '''
    df['signal_' + feature] = df['signal_' + feature].fillna(0)
    df['signal_' + feature] = df['signal_' + feature].astype('int32')
    df['my_strat_' + feature] = df['signal_' + feature] * df['mth_ret']
    return df


def summary(df, indicator, feature):
    '''
    summary [summary]

    Parameters
    ----------
    df : [type]
        [description]
    indicator : [type]
        [description]
    feature : [type]
        [description]
    '''
    cor = df[indicator].corr(df[feature])
    df['my_strat_' + feature].replace(0.0, np.nan, inplace=True)
    sr_my = (df['my_strat_' + feature].mean()*math.sqrt(12)) / \
        df['my_strat_' + feature].std(axis=0)
    print('Sharpe Ratio of ' + indicator + ': ' + str(round(sr_my, 3)))
    print('Corr between ticker and ' + feature + ': ' + str(round(cor, 3)))


def summary_values(df, indicator, feature):
    '''
    summary_values [summary]

    Parameters
    ----------
    df : [type]
        [description]
    indicator : [type]
        [description]
    feature : [type]
        [description]

    Returns
    -------
    [type]
        [description]
    '''
    # for features with no manipulation
    if feature in ('none', 'mean', 'median'):
        df['my_strat_' + feature].replace(0.0, np.nan, inplace=True)

        # sharpe ratio
        sr_my = (df['my_strat_' + feature].mean() * math.sqrt(12)
                 ) / df['my_strat_' + feature].std(axis=0)

        return sr_my
    # features with manipulation
    else:
        df['my_strat_' + feature].replace(0.0, np.nan, inplace=True)

        # sharpe ratio
        sr_my = (df['my_strat_' + feature].mean() * math.sqrt(12)) / \
            df['my_strat_' + feature].std(axis=0)

        # correlation with monthly returns
        # cor = df['mth_ret'].corr(df[feature])
        return sr_my



def summary_stats(df):
    '''
    summary_report [creates summary stats for each ticker]

    Parameters
    ----------
    df : [DataFrame]
        [the end dataframe]

    Returns
    -------
    [DataFrame]
        [returns a new dataframe with the summary report of each ticker]
    '''
    # filter based on my_strat columns
    my_strat = df.filter(regex='my_strat').fillna(np.nan)
    my_signal = df.filter(regex='signal_')

    # set the rows
    rows = constant.SUMMARY_ROWS

    # for calculating hit rates
    # hits = [my_strat.groupby(my_strat[my_strat.columns[i]].apply(lambda x: 'positive' if x > 0 else 'negative' if x < 0 else 'no trade'))[
    #     my_strat.columns[i]].count() for i in range(len(my_strat.columns))]

    # new df
    summary = pd.DataFrame(index=rows, columns=my_strat.columns)

    # get the array values and set
    summary.loc['avg_gain'] = [my_strat[my_strat[my_strat.columns[i]] > 0]
                               [my_strat.columns[i]].mean() for i in range(len(my_strat.columns))]
    summary.loc['avg_loss'] = [my_strat[my_strat[my_strat.columns[i]] < 0]
                               [my_strat.columns[i]].mean() for i in range(len(my_strat.columns))]

    summary.loc['max_gain'] = my_strat.describe().loc['max'].values
    summary.loc['max_loss'] = my_strat.describe().loc['min'].values

    summary.loc['hit_rate'] = [sum(my_signal[my_signal.columns[i]] > 0)/len(
        my_signal[my_signal.columns[i]]) for i in range(len(my_signal.columns))]

    # summary.loc['all_moves'] = [
    #     len(my_strat[my_strat.columns[i]].dropna()) for i in range(len(my_strat.columns))]
    # summary.loc['positive_trades'] = [hits[i][2] for i in range(len(hits))]
    # summary.loc['negative_trades'] = [hits[i][0] for i in range(len(hits))]
    # summary.loc['no_trades'] = [hits[i][1] for i in range(len(hits))]

    summary.loc['all_moves'] = [len(my_signal[my_signal.columns[i]].dropna())
                                for i in range(len(my_signal.columns))]

    summary.loc['positive_trades'] = [my_signal[my_signal.columns[i]].gt(
        0).sum() for i in range(len(my_signal.columns))]

    summary.loc['negative_trades'] = [my_signal[my_signal.columns[i]].lt(
        0).sum() for i in range(len(my_signal.columns))]

    summary.loc['no_trades'] = [my_signal[my_signal.columns[i]].eq(
        0).sum() for i in range(len(my_signal.columns))]

    summary = summary.T

    return summary


def arith_ret(df, feature, indicator):
    '''
    arith_ret [summary]

    Parameters
    ----------
    df : [type]
        [description]
    feature : [type]
        [description]
    indicator : [type]
        [description]
    '''
    df = df.set_index('entry_date')
    fig, ax = plt.subplots(nrows=2, sharex=True, figsize=(15, 8))
    ax[0].plot(df.index, (1+np.cumsum(df['my_strat_' + feature])))
    ax[0].title.set_text('Arithmetic Return on $1 Wager')
    ax[1].bar(df.index, df['my_strat_' + feature], edgecolor='red',
              width=1/(20*len(df.index)))
    ax[1].title.set_text('Returns based on strategy of ' + feature)
    # plt.show()
    fig.savefig(save_results_to + indicator + '/' + 'Returns based on strategy of ' +
                feature + '.png', dpi=300)
    plt.close('all')


def ticker_and_com(df, indicator, comdty):
    '''
    ticker_and_com [summary]

    Parameters
    ----------
    df : [type]
        [description]
    comdty : [type]
        [description]
    '''
    df = df.set_index(df.columns[0])

    fig, ax = plt.subplots(nrows=2, sharex=True, figsize=(15, 8))
    ax[0].plot(df.index, df.entry_price)
    ax[0].title.set_text(comdty)
    ax[1].bar(df.index, df[df.columns[0]],
              edgecolor='red', width=1/(50*len(df.index)))
    ax[1].title.set_text(df.columns[0])
    # plt.show()
    fig.savefig(save_results_to + indicator + '/' + comdty + ' and ' +
                indicator + '.png', dpi=300)
    plt.close('all')
