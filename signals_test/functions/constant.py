'''
    [all constants]
'''

COMDTY = [
    'LP3 COMDTY', 'LA3 COMDTY'
    ]

# all features
FEAT_COLS = [
    '6mn_ma', '12mn_ma', '6mn_minmax', '12mn_minmax',
    'yoy_6mn_minmax', 'yoy_6mn_ma', 'yoy_12mn_minmax',
    'yoy_12mn_ma', 'mean', 'median'
    ]

# without none-feature
# for data sources containing PMI
FEAT_COLS_NONE = [
    '6mn_ma', '12mn_ma', '6mn_minmax', '12mn_minmax',
    'yoy_6mn_minmax', 'yoy_6mn_ma', 'yoy_12mn_minmax',
    'yoy_12mn_ma', 'mean', 'median', 'none'
    ]

SUMMARY_ROWS = [
    'hit_rate', 'positive_trades', 'negative_trades',
    'no_trades', 'all_moves', 'max_gain', 'max_loss', 
    'avg_gain', 'avg_loss'
    ]

# for stats.py
PARENT = './data/'

STATS_PATH = [
    './data/all_tick.csv', './data/overall_summary_report.pkl',
    './data/all_tickers_sharpe_ratio_copy.csv'
    ]

STATS_PATH_ALUM = [
    PARENT + COMDTY[1] + '_results.pkl', PARENT + COMDTY[1] + '_summary_report.pkl',
    PARENT + COMDTY[1] + '_database.pkl', PARENT + COMDTY[1] + '_sharpe_ratio.csv'
    ]

STATS_NEWCOLS = [
    'hit_rate', 'positive_trades', 'negative_trades', 'no_trades',
    'all_moves', 'max_gain', 'max_loss', 'avg_gain', 'avg_loss'
    ]

# main script paths
ALL_TICKERS_PATH = './data/all_tickers.pkl'

COPPER_PATH = './data/LP3 COMDTY.pkl'

ALUM_PATH = './data/LA3 COMDTY.pkl'

ALL_TICKERS_ALUM_PATH = './data/alum.pkl'

FORMAT = '.pkl'

### SOY ########
SOY_TICKERS_PATH = [
    './data/soy_mth_tickers_data.pkl' 
]

SOY_COMDTY = [
    'S 3 Comdty', 'BO3 Comdty',
    'SM3 Comdty'
]

# soy comdty data 
SOY_COM_DATA = [
    PARENT + SOY_COMDTY[0] + FORMAT, 
    PARENT + SOY_COMDTY[1] + FORMAT, 
    PARENT + SOY_COMDTY[2] + FORMAT
]

# path for soy stats
STATS_PATH_SOY = [
    PARENT + SOY_COMDTY[0] + '_results.pkl', PARENT + SOY_COMDTY[0] + '_summary_report.pkl',
    PARENT + SOY_COMDTY[0] + '_database.pkl', PARENT + SOY_COMDTY[0] + '_sharpe_ratio.csv'
    ]


STATS_PATH_SOY_1 = [
    PARENT + SOY_COMDTY[1] + '_results.pkl', PARENT + SOY_COMDTY[1] + '_summary_report.pkl',
    PARENT + SOY_COMDTY[1] + '_database.pkl', PARENT + SOY_COMDTY[1] + '_sharpe_ratio.csv'
    ]

STATS_PATH_SOY_2 = [
    PARENT + SOY_COMDTY[2] + '_results.pkl', PARENT + SOY_COMDTY[2] + '_summary_report.pkl',
    PARENT + SOY_COMDTY[2] + '_database.pkl', PARENT + SOY_COMDTY[2] + '_sharpe_ratio.csv'
    ]




