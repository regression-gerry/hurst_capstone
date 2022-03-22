'''
    [Sorting out tickers into different frequencies]
'''

import numpy as np
import pandas as pd

comdty = pd.read_pickle('./new_data/copper_updated_2021.pkl')

# d = []
# for i in range(len(comdty)):
#     df = soy[i]
#     # based on the last two values
#     diff = df[df.columns[0]][-1:].values - df[df.columns[0]][-2:-1].values
#     diff_days = int((diff/np.timedelta64(1, 'D')).item(0))
#     d.append(diff_days)

f = []
for i in range(len(comdty)):
    df = comdty[i]
    freq = pd.infer_freq(df[df.columns[0]])
    f.append(freq)

tickers = [comdty[i].columns[1] for i in range(len(comdty))]

dic = {'ticker':tickers, 'freq':f}

print('breakpoint')