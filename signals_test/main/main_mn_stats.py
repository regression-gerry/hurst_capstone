'''
    [Get stats and reshape dfs into summary sheet
    - changed on 8/4 for steel and iron ore series
    ]
'''

# libraries
from new_scripts.base import *
startTime = time.time()

data_type = 'proj'

comdty = ['alum', 'copper', 'soy_s2', 'soymeal_sm2', 'soyoil_b02']

soy_sr = pd.read_pickle('./new_data/steelcaixin_sr_daily_bins.pkl')

# soy_sr = pd.read_pickle('./new_data/sr_60_long_alum.pkl')

# create empty dataframe with columns
bins = ['1', '2', '3', '4'] #### BINS CHANGED -> to revert
MN_MINMAX_FEATURES = [
    # '12mn_minmax', 'yoy_12mn_minmax',
    # '24mn_minmax', '36mn_minmax',
    # '60mn_minmax'
    '3mn_minmax', '6mn_minmax', '9mn_minmax',
    '12mn_minmax'
]


MN_MINMAX_FEATURES = [MN_MINMAX_FEATURES[j] + "_bin" + bins[i]
                      for i in range(len(bins)) for j in range(len(MN_MINMAX_FEATURES))]

MN_MINMAX_FEATURES = sorted(MN_MINMAX_FEATURES)

MN_MINMAX_FEATURES_COUNT = [MN_MINMAX_FEATURES[i] +
                            "_count" for i in range(len(MN_MINMAX_FEATURES))]
MN_MINMAX_FEATURES_COUNT = sorted(MN_MINMAX_FEATURES_COUNT)

for i in range(len(MN_MINMAX_FEATURES_COUNT)):
    MN_MINMAX_FEATURES.append(MN_MINMAX_FEATURES_COUNT[i])

MN_MINMAX_FEATURES.insert(0, 'ticker')

# empty dataframe
sr_stats = pd.DataFrame(columns=MN_MINMAX_FEATURES)

# get list of tickers to include in dataframe
mn_tickers = []
for i in range(len(soy_sr)):
    if soy_sr[i].empty:
        continue
    else:
        mn_tickers.append(soy_sr[i].columns[0])

# loop in individual dataframes
for i in range(len(soy_sr)):
    if soy_sr[i].empty:
        continue
    sr = soy_sr[i].iloc[:, 1:3].reset_index(drop=True)
    sr = sr.T.reset_index(drop=True)
    sr.columns = sr.iloc[0]
    sr = sr.drop(sr.index[0]).reset_index(drop=True)

    bin_count = soy_sr[i].iloc[:, [1, 3]].reset_index(drop=True)
    bin_count['bin_features'] = bin_count['bin_features'].apply(
        lambda x: x + '_count')
    bin_count = bin_count.T.reset_index(drop=True)
    bin_count.columns = bin_count.iloc[0]
    bin_count = bin_count.drop(bin_count.index[0]).reset_index(drop=True)
    sr_bin = pd.concat([bin_count, sr], axis=1).reindex(bin_count.index)

    sr_stats = sr_stats.append([sr_bin])

# store tickers to dataframe
sr_stats.ticker = mn_tickers

if __name__ == "__main__":
    sr_stats.to_csv('./new_data/' 'steel' '_mn_caixin_data_stats.csv')
    # sr_stats.to_csv('./new_data/sr_60_long_alum_stats.csv')
print('code ran in {0}s'.format(time.time() - startTime))