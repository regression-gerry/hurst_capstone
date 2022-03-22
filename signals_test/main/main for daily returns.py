'''
     [script for upsampling mn/wk data to daily
     - comdty ret
     - pickle file
     - currently suitable for wasde mn data only
     ]

'''
# libraries
from new_scripts.base import *
import sys
sys.path.extend(['C:\\Users\\gerry.zeng\\projects', 'C:\Users\gerry.zeng\projects'])
startTime = time.time()

# -----------------------------------------------
# 1. read in data files
# read tickers
# -----------------------------------------------
# define file name
params = {
    'folder': 'new_data',
    'pickle file': 'ss_dd_proj_est_updated_with_rdates',
    'comdty returns': 'comdty_ret',
    'start date': '1995-01-01',
    'data type': 'proj',
    'bins': ['1', '2', '3', '4'],
    'comdty': ['soy_s2', 'soyoil_b02', 'soymeal_sm2'],

    # work in progress
    'monthly': [ns.MONTHS, ns.MONTHS_D, ns.MN_MINMAX_FEATURES],
    'weekly': [ns.WEEKS, ns.WEEKS_D, ns.WK_MINMAX_FEATURES],
    'freq': ['wk', 'mn']
}

all_tickers = pd.read_pickle(
    './' + params['folder'] + '/' + params['pickle file'] + '.pkl'
)

# read in comdty returns
comdty_cols = [0, 16, 20, 24]  # date|s2|b02|sm2
ret = ns.comdty_parser(params['folder'], params['comdty returns'], comdty_cols)
frequency = params['freq'][1]  # set frequency to wk or mn

# ----------------------------------------------------------------
# 2. Get returns between entry_date + Bday(1) and entry_date.shift
# store all dfs with returns series
# ----------------------------------------------------------------
wrangled_data = []
# iterate through entire list of dataframes
for dataframe in all_tickers:
    df = dataframe

    # set date to be after jan 1995 (for wasde)
    df = df[df[df.columns[0]] >= params['start date']].reset_index(drop=True)

    # YoY changes for mn or wk
    if frequency == 'mn':  # monthly
        df = df.replace(0, np.nan)
        df = ns.yoy_mn(df, 1, 0) # additional date_col
    elif frequency == 'wk':  # weekly
        df = df.set_index(df.columns[0]).resample(
            'W-{:%a}'.format(df.date[0])).asfreq().fillna(np.nan).reset_index()
        df['week'] = df['date'].dt.week
        df.replace({'week': 53}, 52, inplace=True)
        # date,proj,est,
        df['yoy'] = df.groupby(['week'])[df.columns[1]].pct_change()
        df = df.drop(['week'], axis=1)

    df['entry_date'] = df[df.columns[0]] + BDay(1)

    # features
    df = ns.yoy_minmax(df, 'mn', ns.MONTHS[0], ns.MONTHS_D[0])
    for m_i, m_j in zip(ns.MONTHS, ns.MONTHS_D):
        df = ns.period_minmax(df, 'mn', 1, m_i, m_j)

    # create signal_bins and columns
    for feature in ns.MN_MINMAX_FEATURES:
        df = ns.signal_bins(df, feature)

    df.set_index(df['entry_date'], inplace=True)

    # combine with daily returns
    combined_df = pd.concat([ret, df], axis=1).reindex(ret.index)
    combined_df.iloc[:, 0:3] = combined_df.iloc[:, 0:3]/100  # make absolute

    # find columns whose name contains specific strings
    combined_df.loc[:, combined_df.columns.str.contains(
        "bins")] = combined_df.loc[:, combined_df.columns.str.contains("bins")].fillna(method='ffill')
    # shift the bins columns by 1 to align with returns
    combined_df.loc[:, combined_df.columns.str.contains(
        "bins")] = combined_df.loc[:, combined_df.columns.str.contains("bins")].shift(1)

    # choose the correct comdty returns
    for feature in ns.MN_MINMAX_FEATURES:
        # get the signals
        combined_df = ns.my_strat_bins(
            combined_df, feature, combined_df.columns[0])

    # store data
    wrangled_data.append(combined_df)
    print('block ran in {0}s'.format(time.time() - startTime))

# ----------------------------------------------------------
# 3. Get the sharpe ratio for all bins (1,2,3,4) and counts
# ----------------------------------------------------------
sr_bins = []  # store sharpe ratio
# new loop for wrangled data
for j in range(len(wrangled_data)):
    # set dataframe as com (comdty)
    com = wrangled_data[j]

    bin_no = params['bins']
    bin_features = []
    minmax_count = []
    minmax_sr = []
    for i_bin in range(len(bin_no)):
        for j_minmax in range(len(ns.MN_MINMAX_FEATURES)):
            # replace zeros with nan for correct SR calc
            com['my_strat_' + ns.MN_MINMAX_FEATURES[j_minmax] + '_bins' +
                bin_no[i_bin]].replace(0.0, np.nan, inplace=True)
            minmax_count.append(
                com['my_strat_' + ns.MN_MINMAX_FEATURES[j_minmax] + '_bins' + bin_no[i_bin]].count())

            # sharpe ratio (daily, 260 trading days based on 30yrs mean days count)
            # or 12 for mn trades
            sr = (com['my_strat_' + ns.MN_MINMAX_FEATURES[j_minmax] + '_bins' + bin_no[i_bin]].mean() *
                  math.sqrt(12))/com['my_strat_' + ns.MN_MINMAX_FEATURES[j_minmax] + '_bins' + bin_no[i_bin]].std()
            minmax_sr.append(sr)

            # bin features string
            s = ns.MN_MINMAX_FEATURES[j_minmax] + '_bin' + bin_no[i_bin]
            bin_features.append(s)

    # store SR results
    results_df = pd.DataFrame({
        # 4. proj, 5. est
        com.columns[4]: pd.Series([], dtype=pd.StringDtype()),
        'bin_features': pd.Series(bin_features),
        'bin_sr': pd.Series(minmax_sr),
        'bin_count': pd.Series(minmax_count)
    })

    results_df = results_df.sort_values(by='bin_features', ascending=True,
                                        axis=0).reset_index(drop=True)

    sr_bins.append(results_df)
    # 4. proj, 5. est
    print(com.columns[4] + ' parsed')
    print('block ran in {0}s'.format(time.time() - startTime))

# -------------------------------------------
# 4. Save results
# -------------------------------------------
if __name__ == "__main__":
    # change to est or proj
    with open('./new_data/' + params['comdty'][0] + '_mn' + params['data type'] + '_sr_daily_bins.pkl', 'wb') as f:
        pickle.dump(sr_bins, f)

    with open('./new_data/' + params['comdty'][0] + '_mn' + params['data type'] + '_wrangled_daily_data.pkl', 'wb') as f:
        pickle.dump(wrangled_data, f)

print('code finished in {0}s'.format(time.time() - startTime))
