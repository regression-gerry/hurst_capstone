'''
     [script to get weekly data]

'''
# libraries
from base import *

startTime = time.time()

# 1. read in data files
# read tickers
folder = 'new_data'
file_name = 'alum_wk_2021'
all_tickers = pd.read_pickle('./' + folder + '/' + file_name + '.pkl')

# read in comdty returns
comdty_cols = [0, 83]  # date|s2|b02|sm2
ret = ns.comdty_parser('new_data', 'comdty_ret', comdty_cols)
ret_list = ret.columns.to_list()

# ----------------------------------------------------------------
# 2. Get returns between entry_date + Bday(1) and entry_date.shift
# store all dfs with returns series
# ----------------------------------------------------------------
dfs = []
# End df -> S 2 Comdty|date|ticker|entry_date|week|month|yoy|ret
for i in range(len(all_tickers)):
    # date|ticker
    df = all_tickers[i]

    # for enso
    # df = df.iloc[:, [0,1]]
    # df.columns = ['date', df.columns[1]]

    # set all to be after jan 1995
    # df = df[df[df.columns[0]] >= '1990-01-01'].reset_index(drop=True)

    # resample as nan
    df = df.set_index(df.columns[0]).resample(
        'W-{:%a}'.format(df.date[0])).asfreq().fillna(np.nan).reset_index()

    df['entry_date'] = df['date'] + BDay(1)

    # create week column (week of the month) and month column
    # df['week'] = df['date'].apply(lambda dt: int(math.ceil(dt.day/7.0)))
    # df['month'] = df['date'].dt.month
    # df['yoy'] = df.groupby(['month', 'week'])[df.columns[1]].pct_change()
    df['week'] = df['date'].dt.week
    df.replace({'week': 53}, 52, inplace=True)
    df['yoy'] = df.groupby(['week'])[df.columns[1]].pct_change()
    df = df.drop(['week'], axis=1)

    # set arbitrary returns columns
    for i in range(len(ret.columns)):
        df[ret.columns[i] + '_ret'] = np.random.randint(100)

    # combine comdty returns with weekly data
    df.set_index(df['entry_date'], inplace=True)
    df = pd.concat([ret, df], axis=1).reindex(ret.index)

    # date range to sum up returns for weekly data
    starting_dates = df.loc[df.index == df.entry_date].index + BDay(1)
    ending_dates = df.loc[df.index == df.entry_date].index[1:]
    ending_dates = ending_dates.append(ret.index[-1:])

    # store returns series
    # all returns
    all_ret = [[np.sum(df.loc[(df.index >= starting_dates[i]) & (
                df.index <= ending_dates[i])][df.columns[j]]) for i in range(len(starting_dates))] for j in range(len(ret.columns))]

    # df.ret.isnotnull, on ret column, replace as ret series
    for returns, pos in zip(all_ret, ret_list):
        df.loc[~df[pos + '_ret'].isnull(), [pos + '_ret']] = returns

    # set df to be those data only
    df = df.loc[~df[ret_list[0] + '_ret'].isnull()]
    df = df.reset_index(drop=True)
    df = df.drop(columns=ret.columns.to_list(), axis=1)

    # on the weekly returns based on how many comdty
    # take the last column
    df.iloc[:, -1:] = df.iloc[:, -1:] / 100

    # append dfs to dataframe
    dfs.append(df)
    print('block ran in {0}s'.format(time.time() - startTime))

#
sr_bins = []  # store sharpe ratio
wrangled_week_data = []  # store wrangled data
# new loop for wrangled data
for j in range(len(dfs)):
    # ns.WEEKS = [52, 104, 156, 260]
    d = dfs[j]

    # only on 52 weeks (1 year)
    d = ns.yoy_minmax(d, 'wk', ns.WEEKS[0], ns.WEEKS_D[0])

    # apply period minmax to dataframe
    for m_i, m_j in zip(ns.WEEKS, ns.WEEKS_D):
        # col_no = 2 for ticker
        d = ns.period_minmax(d, 'wk', 1, m_i, m_j)

    # columns to select comdty
    s2 = 0
    b2 = 1
    sm2 = 2

    # apply signals (1, 0) and strat returns
    for iii in range(len(ns.WK_MINMAX_FEATURES)):
        d = ns.signal_bins(d, ns.WK_MINMAX_FEATURES[iii])

        # CHANGE THE COMDTY HERE
        ret_column = ret_list[s2] + '_ret'
        d = ns.my_strat_bins(d, ns.WK_MINMAX_FEATURES[iii], ret_column)

    wrangled_week_data.append(d)

    # binning for sharpe ratio
    bin_no = ['1', '2', '3', '4']

    bin_features = []
    minmax_count = []
    minmax_sr = []

    # minmax_count and minmax_sr
    for i_bin in range(len(bin_no)):
        for j_minmax in range(len(ns.WK_MINMAX_FEATURES)):
            d['my_strat_' + ns.WK_MINMAX_FEATURES[j_minmax] + '_bins' +
                bin_no[i_bin]].replace(0.0, np.nan, inplace=True)
            minmax_count.append(
                d['my_strat_' + ns.WK_MINMAX_FEATURES[j_minmax] + '_bins' + bin_no[i_bin]].count())

            # sharpe ratio
            sr = (d['my_strat_' + ns.WK_MINMAX_FEATURES[j_minmax] + '_bins' + bin_no[i_bin]].mean() *
                  math.sqrt(12))/d['my_strat_' + ns.WK_MINMAX_FEATURES[j_minmax] + '_bins' + bin_no[i_bin]].std()
            minmax_sr.append(sr)

            # bin features string
            s = ns.WK_MINMAX_FEATURES[j_minmax] + '_bin' + bin_no[i_bin]
            bin_features.append(s)

    # dataframe for storing sr results
    d2 = pd.DataFrame({
        d.columns[1]: pd.Series([], dtype=pd.StringDtype()),
        'bin_features': pd.Series(bin_features),
        'bin_sr': pd.Series(minmax_sr),
        'bin_count': pd.Series(minmax_count)
    })

    d2 = d2.sort_values(by='bin_features', ascending=True,
                        axis=0).reset_index(drop=True)

    # d2 = d2[~(d2.bin_features.str.contains("bin2|bin3")) & (d2.bin_count > 50)]

    sr_bins.append(d2)
    print(d.columns[1])
    print('block ran in {0}s'.format(time.time() - startTime))


# -------------------------------------------
# 4. Save results
# -------------------------------------------
comdty = ['alum', 'soy_s2', 'soymeal_sm2', 'soyoil_b02']
if __name__ == "__main__":
    # sharpe ratio results unwrangled
    with open('./new_data/' + comdty[0] + '_w_sr_bins.pkl', 'wb') as f:
        pickle.dump(sr_bins, f)

    # list of wrangled df
    with open('./new_data/' + comdty[0] + '_w_wrangled_data.pkl', 'wb') as f:
        pickle.dump(wrangled_week_data, f)
