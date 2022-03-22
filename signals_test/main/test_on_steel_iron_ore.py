'''
    [- 3,6,9,12 minmax
    - no min_period required
    - on steel and iron ore]
'''
from past_scripts.func import get_entry_price, get_exit_price
from new_scripts.base import *
import sys

sys.path.extend(['C:\\Users\\gerry.zeng\\projects'])
path = './projects/a.txt'
sys.stdout = open(path, 'w')

startTime = time.time()

# base comdty
base_comdty = pd.read_pickle('./new_data/copper_alum_steel.pkl')
copper_mapping = dict(base_comdty[0].values)
alum_mapping = dict(base_comdty[1].values)
steel_mapping = dict(base_comdty[2].values)

# steel pmi
excel_path = './new_data/Cai Xin PMI Adjusted.xlsx'
sheet = 'Sheet 1'
period = [3, 6, 9, 12]
features = [
    '3mn_minmax', '6mn_minmax', '9mn_minmax',
    '12mn_minmax'
]


def pmi_reader(path, sheet):
    df = pd.read_excel(path, sheet_name=sheet)
    df = df.set_index('Date')
    return [df.iloc[:, i:i+1] for i in range(0, len(df.columns)) if i+1 <= len(df.columns)]

wrangled_pmi = []
for ticker in pmi_reader(excel_path, sheet=sheet):
    ticker['entry_date'] = ticker.index + BDay()
    ticker = get_entry_price(ticker, steel_mapping)

    for mn in period:
        ticker = ns.period_minmax(ticker, 'mn', 0, mn, d=None)

    ticker['ret'] = ticker.entry_price.pct_change().shift(-1)

    for feature in features:
        ticker = ns.signal_bins(ticker, feature)
        ticker = ns.my_strat_bins(ticker, feature, 'ret')
    
    wrangled_pmi.append(ticker)
    print('block ran in {0}s'.format(time.time() - startTime))
    
# ----------------------------------------------------------
# 3. Get the sharpe ratio for all bins (1,2,3,4) and counts
# ----------------------------------------------------------
sr_bins = []  # store sharpe ratio
# new loop for wrangled data
for j in range(len(wrangled_pmi)):
    # set dataframe as com (comdty)
    com = wrangled_pmi[j]

    bin_no = ['1', '2', '3', '4']
    bin_features = []
    minmax_count = []
    minmax_sr = []
    for i_bin in range(len(bin_no)):
        for j_minmax in range(len(features)):
            # replace zeros with nan for correct SR calc
            com['my_strat_{}_bins{}'.format(features[j_minmax], bin_no[i_bin])].replace(0.0, np.nan, inplace=True)
            minmax_count.append(
                com['my_strat_' + features[j_minmax] + '_bins' + bin_no[i_bin]].count())

            # sharpe ratio (daily, 260 trading days based on 30yrs mean days count)
            # or 12 for mn trades
            sr = (com['my_strat_' + features[j_minmax] + '_bins' + bin_no[i_bin]].mean() *
                  math.sqrt(12))/com['my_strat_' + features[j_minmax] + '_bins' + bin_no[i_bin]].std()
            minmax_sr.append(sr)

            # bin features string
            s = features[j_minmax] + '_bin' + bin_no[i_bin]
            bin_features.append(s)

    # store SR results
    results_df = pd.DataFrame({
        # 4. proj, 5. est
        com.columns[0]: pd.Series([], dtype=pd.StringDtype()),
        'bin_features': pd.Series(bin_features),
        'bin_sr': pd.Series(minmax_sr),
        'bin_count': pd.Series(minmax_count)
    })

    results_df = results_df.sort_values(by='bin_features', ascending=True,
                                        axis=0).reset_index(drop=True)

    sr_bins.append(results_df)
    # 4. proj, 5. est
    print(com.columns[0] + ' parsed')
    print('block ran in {0}s'.format(time.time() - startTime))

with open('./new_data/' + 'steel' + 'caixin_sr_daily_bins.pkl', 'wb') as f:
        pickle.dump(sr_bins, f)

with open('./new_data/' + 'steel' + 'caixin_wrangled_daily_data.pkl', 'wb') as f:
    pickle.dump(wrangled_pmi, f)

print('done')
