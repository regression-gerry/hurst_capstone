import sys
import logging
import pandas as pd
import numpy as np
import requests
from datetime import date

class DataProcessCoinAPI:
    
    def __init__(self, name, api_key, coin_attributes):
        logging.basicConfig(
            level=logging.DEBUG,
            format='{asctime} {levelname: <8} {message}',
            style='{',
            handlers=[
                logging.FileHandler(
                    f'./logs/data_pull {date.today()}.log',
                    mode='a', encoding=None, delay=False
                ),
                logging.StreamHandler(sys.stdout)
            ],
            force=True
        )
        self.logger = logging
        self.name = name
        self.api_key = api_key
        
        # read in df for coin attributes
        self.coin_attributes = coin_attributes
    
    def connect(self):
        
        """_summary_
        - get list of perp and spot
        - store as lists
        - define start date for first coin and first API
        - 
        
        """
    @staticmethod    
    def store_data(df: pd.DataFrame, condition: str, path: str):
        # store new data to new pickle file
        if condition == 'new':
            df.to_pickle(path)
        # store new data to existing pickle file
        else:
            existing_data = pd.open_pickle(path)
            existing_data = existing_data.append(df)
            existing_data.to_pickle(path)
        
    def get_data(self):
        
    
    def get_data(df, start, count_limit, ticker, api_key):
        count = 0
        condition = 0
        while True:
            if df.empty:
                # define start date
                start_dt = '2022-10-23T00:00:00'
            else:
                # set start date
                start_dt = start
            try:
                if count == 0 and condition == 0:
                    url = f'https://rest.coinapi.io/v1/ohlcv/{ticker}/history?period_id=1HRS&time_start={start_dt}'
                    headers = {'X-CoinAPI-Key' : f'{api_key}'}
                    response = requests.get(url, headers=headers)
                    if response.status_code == 200:
                        data = pd.DataFrame(response.json())
                        if data.empty:
                            break
                        data['ticker'] = ticker
                        df = df.append(data)
                        print('1st iteration done')
                        condition = 1
                        count += 1
                    elif response.status_code != 200:
                        print('error in retrieving data')
                        break
                elif condition == 1 and count > 0 and count < count_limit:
                    start_dt_revised = data.iloc[-1:,1:2].values[0][0][0:19]
                    url = f'https://rest.coinapi.io/v1/ohlcv/{ticker}/history?period_id=1HRS&time_start={start_dt_revised}'
                    headers = {'X-CoinAPI-Key' : f'{api_key}'}
                    response = requests.get(url, headers=headers)
                    if response.status_code == 200:
                        data = pd.DataFrame(response.json())
                        if data.empty:
                            break
                        data['ticker'] = ticker
                        df = df.append(data)
                        print(f'{count} iteration')
                        condition = 1
                        count += 1
                    elif response.status_code != 200:
                        print('no data for n run')
                        break
                elif count == count_limit:
                    break
                        
            except Exception as e:
                print(e)
                break
        
        return df
        