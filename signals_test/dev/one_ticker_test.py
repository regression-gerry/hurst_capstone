'''
    [Script to test on one ticker - e.g. stocks % usage ]
'''


# libraries
# libraries
import func as f
import features
from signals import *
import constant
import time
# import sys
import numpy as np
import pandas as pd
from pickle import load, dump
from pandas.tseries.offsets import BDay
from math import sqrt
import matplotlib.pyplot as plt
import os
import seaborn as sns
sns.set_theme(style="darkgrid")
pd.options.mode.chained_assignment = None

startTime = time.time()

# set ticker to read
ticker = pd.read_csv('')

# setting comdty dataframe to map to
com = pd.read_pickle(constant.SOY_COM_DATA[0]) # S 3 comdty

# com = pd.read_pickle('./data/LP3 COMDTY.pkl')
# mapping = dict(com[['date', 'LA3 COMDTY']].values)
mapping = dict(com.values) # set mapping