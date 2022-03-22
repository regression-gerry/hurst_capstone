'''
     [script to get monthly data output; for base metals

     Assumptions:
     - SMM or other data sources have arbitrary release dates
     - robustness of release_dates
     - added in release_dates columns to trade on
     ]

'''
# libraries
from dev.base import *

startTime = time.time()

# -----------------------------------------------
# 1. read in data files
# read tickers
# -----------------------------------------------
# define file name
folder = 'new_data'
file_name = 'alum_mn_2021'
# date | ticker
all_tickers = pd.read_pickle('./' + folder + '/' + file_name + '.pkl')

# read in comdty returns
comdty_cols = [0, 83]  # date|LP2, 87 or LA2, 83
ret = ns.comdty_parser('new_data', 'comdty_ret', comdty_cols)
ret_list = ret.columns.to_list()  # list of comdty to be included

# ----------------------------------------------------------------
# 2. Get returns between entry_date + Bday(1) and entry_date.shift
# store all dfs with returns series
# ----------------------------------------------------------------
dfs = []
# End df -> S 2 Comdty|date|ticker|entry_date|week|month|yoy|ret
for i in range(len(all_tickers)):
    # Start df -> date 0|ticker, 1
    df = all_tickers[i]

    df = df.set_index(df.columns[0]).resample(
        'M').asfreq().fillna(np.nan).reset_index()

    df['release_date'] = df[df.columns[0]] + \
        pd.DateOffset(15)  # 15 days forward as r date

    # df[df.columns[0]] = df[df.columns[0]].apply(lambda dt: dt.replace(day=12))

    # # set date to be after jan 1990
    df = df[df[df.columns[0]] >= '1990-01-01'].reset_index(drop=True)

    # replace 0 to np.nan on indicator columns -> only for monthly data
    # df = df.replace(0, np.nan) # resample fillna

    df['entry_date'] = df['release_date'] + BDay(1)

    # create week column (week of the month) and month column
    # df['week'] = df[df.columns[0]].apply(lambda dt: int(math.ceil(dt.day/7.0)))
    # df['month'] = df[df.columns[0]].dt.month

    # do a yoy change on indicator ****** change to est indicator
    # df['yoy'] = df.groupby(['month', 'week'])[df.columns[1]].pct_change()

    # YoY on ticker
    # date 0|ticker 1
    df = ns.yoy_mn(df, 1)

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
    # starting_dates = starting_dates[:-1]

    # all returns
    all_ret = [[np.sum(df.loc[(df.index >= starting_dates[i]) & (
                df.index <= ending_dates[i])][df.columns[j]]) for i in range(len(starting_dates))] for j in range(len(ret.columns))]

    # df.ret.isnotnull, on ret column, replace as ret series
    for returns, pos in zip(all_ret, ret_list):
        df.loc[~df[pos + '_ret'].isnull(), [pos + '_ret']] = returns

    df = df.loc[~df[ret_list[0] + '_ret'].isnull()]
    df = df.reset_index(drop=True)
    df = df.drop(columns=ret.columns.to_list(), axis=1)

    # date,0|ticker,1|entry_date,2|release_date,3
    # make it absolute
    df.iloc[:, 4:] = df.iloc[:, 4:] / 100
    dfs.append(df)
    # print(df.columns)
    print('block ran in {0}s'.format(time.time() - startTime))


# ----------------------------------------------------------
# 3. Get the sharpe ratio for all bins (1,2,3,4) and counts
# ----------------------------------------------------------
sr_bins = []  # store sharpe ratio
wrangled_mn_data = []  # store wrangled data
# new loop for wrangled data
for j in range(len(dfs)):
    d = dfs[j]

    # only on 12 months
    d = ns.yoy_minmax(d, 'mn', ns.MONTHS[0], ns.MONTHS_D[0])

    # apply period minmax to dataframe
    for m_i, m_j in zip(ns.MONTHS, ns.MONTHS_D):
        # on column 3 -> est data *******
        # on col 2 -> proj data *******
        d = ns.period_minmax(d, 'mn', 1, m_i, m_j)

    # select comdty from ret_list (TEMPORARY FIX)
    la2 = 0

    # apply signals (1, 0) and strat returns
    for iii in range(len(ns.MN_MINMAX_FEATURES)):
        d = ns.signal_bins(d, ns.MN_MINMAX_FEATURES[iii])
        # change for ret_col
        ret_column = ret_list[la2] + '_ret'
        d = ns.my_strat_bins(d, ns.MN_MINMAX_FEATURES[iii], ret_column)

    wrangled_mn_data.append(d)

    # binning for sharpe ratio
    bin_no = ['1', '2', '3', '4']

    bin_features = []
    minmax_count = []
    minmax_sr = []

# func
    # minmax_count and minmax_sr
    for i_bin in range(len(bin_no)):
        for j_minmax in range(len(ns.MN_MINMAX_FEATURES)):
            d['my_strat_' + ns.MN_MINMAX_FEATURES[j_minmax] + '_bins' +
                bin_no[i_bin]].replace(0.0, np.nan, inplace=True)
            minmax_count.append(
                d['my_strat_' + ns.MN_MINMAX_FEATURES[j_minmax] + '_bins' + bin_no[i_bin]].count())

            # sharpe ratio
            sr = (d['my_strat_' + ns.MN_MINMAX_FEATURES[j_minmax] + '_bins' + bin_no[i_bin]].mean() *
                  math.sqrt(12))/d['my_strat_' + ns.MN_MINMAX_FEATURES[j_minmax] + '_bins' + bin_no[i_bin]].std()
            minmax_sr.append(sr)

            # bin features string
            s = ns.MN_MINMAX_FEATURES[j_minmax] + '_bin' + bin_no[i_bin]
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

    # print column name
    print(d.columns[1])
    print('block ran in {0}s'.format(time.time() - startTime))

# -------------------------------------------
# 4. Save results
# -------------------------------------------
comdty = ['alum', 'copper', 'soy_s2', 'soymeal_sm2', 'soyoil_b02']
if __name__ == "__main__":
    # change to est or proj
    with open('./new_data/' + comdty[0] + '_mn' + '_sr_bins.pkl', 'wb') as f:
        pickle.dump(sr_bins, f)

    with open('./new_data/' + comdty[0] + '_mn' + '_wrangled_data.pkl', 'wb') as f:
        pickle.dump(wrangled_mn_data, f)

print('code finished in {0}s'.format(time.time() - startTime))
