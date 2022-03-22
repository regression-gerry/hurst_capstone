from datetime import datetime
import numpy as np
import pandas as pd

# for tickers with release dates
def release_dates(df):
    '''
    release_dates 

    Parameters
    ----------
    df : [DataFrame]
        [inital df]

    Returns
    -------
    [DataFrame]
        [adds release dates column; 
        either from bloomberg or pseudo release dates]
    '''
    dates_list = list(df['period_end'].apply(
        lambda x: x.strftime('%Y%m%d')))

    release_dates = [blp.bdp([indicator], flds=[
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
# function not needed
def release_date_test(df, d):
    '''
    Arguments:
    - df :: DataFrame[['period_end', 'indicator']]
    - d : no. of days

    Returns: 
    - DataFrame[['period_end', 'indicator', 'release_date_test']]
    '''
    df['release_date_test'] = df[df.columns[0]] + pd.DateOffset(days=d)
    df['entry_date'] = df['release_date_test'] + BDay(1)
    return df


# entry price
def get_entry_price(df, mapping):
    '''
    Arguments:
    - df :: DataFrame[['period_end', 'indicator', 'release_date']]
    - mapping : dictionary of commodity dates and prices

    Returns: 
    - DataFrame[['period_end', 'indicator', 'release_date', 'entry_price']]
    '''
    df['entry_price'] = df['entry_date'].map(mapping)
    for _ in range(6):
        df.loc[df['entry_price'].isnull(
        ), 'entry_date'] = df.loc[df['entry_price'].isnull(), 'entry_date'] + BDay(1)
        df['entry_price'] = df['entry_date'].map(mapping)
    return df


# exit price
def get_exit_price(df, mapping, n):
    '''
    Arguments:
    - df :: DataFrame[['period_end', 'indicator', 'release_date']]
    - mapping : dictionary of commodity dates and prices
    - n : no. of months

    Returns: 
    - DataFrame[['period_end', 'indicator', 'release_date', 'entry_price', 'exit_price']]
    '''
    df['exit_date'] = df['entry_date'] + pd.DateOffset(months=n)
    df['exit_price'] = df['exit_date'].map(mapping)
    for _ in range(6):
        df.loc[df['exit_price'].isnull(
        ), 'exit_date'] = df.loc[df['exit_price'].isnull(), 'exit_date'] + BDay(1)
        df['exit_price'] = df['exit_date'].map(mapping)
    return df