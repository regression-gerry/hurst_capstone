import time
import hmac

import sqlalchemy
from requests import Request, Session
from datetime import datetime
import pandas as pd
import numpy as np
import logging
import sys
from sqlalchemy import create_engine
# sys.path.extend(['../_crypto'])

class FTXData():
    
    def __init__(self, market, sec, start, end, api, apisecret):
        logging.basicConfig(
                    level=logging.DEBUG,
                    format='{asctime} {levelname: <8} {message}',
                    style='{',
                    handlers=[
                        logging.FileHandler("../logs/data_connector_log.log"),
                        logging.StreamHandler(sys.stdout)
                    ]
                )
        self.logger = logging
        self.market = market
        self.sec = sec
        self.start = start
        self.end = end
        self.api = api
        self.apisecret = apisecret
        
        
    # def get_api(self):
    #     self.api = api
    #     self.api_secret = api_secret
        
    # def get_market_attributes(self):
    #     self.symbols = []
    #     self.market = market
    #     self.sec = sec
    #     self.start = start
    #     self.end = end
    
    def get_data(self):
        try:
            ts = int(time.time() * 1000)
            ## add another line here to test get request as 200 response
            request = Request(
                'GET',
                f'https://ftx.com/api/markets/{self.market}/candles?resolution={self.sec}&start_time={self.start}&end_time={self.end}'
            )
            prepared = request.prepare()
            signature_payload = f'{ts}{prepared.method}{prepared.path_url}'.encode()
            signature = hmac.new(self.apisecret.encode(), signature_payload, 'sha256').hexdigest()
        except Exception:
            # usually connection errors here
            logging.DEBUG('Get request has error')

        prepared.headers['FTX-KEY'] = self.api
        prepared.headers['FTX-SIGN'] = signature
        prepared.headers['FTX-TS'] = str(ts)
        
        response = Session().send(prepared)
        response = response.json()['result']
        
        self.data = pd.DataFrame(response)
        self.data['symbol'] = self.market
    def insert_data(self):
        engine = sqlalchemy.create_engine(
            'sqlite://D:/Downloads/Worldquant Uni/Capstone/_crypto/sqlite/crypto.db',
            echo=False
        )
        self.data.to_sql('timeseries', con=engine, if_exists='append')
        
if __name__ == "__main__":
    api = 'qSt6XBJujWQmfOB0hFp0JX7nVM_tqz4iZJ_wR57m'
    apisecret = 'OYBcxl2FQ_eDboeX7B31owUB8afgLeZyy-q2Oz7w'

    start = datetime.timestamp(datetime(2021, 1, 1))
    end = datetime.timestamp(datetime(2021, 2, 1))
    sec = 1800
    market = 'BTC/USD'
    
    data_obj = FTXData(market, sec, start, end, api, apisecret)
    data_obj.get_data()
    data_obj.insert_data()
    print('break')

    


## under testing
# api = 'qSt6XBJujWQmfOB0hFp0JX7nVM_tqz4iZJ_wR57m'
# apisecret = 'OYBcxl2FQ_eDboeX7B31owUB8afgLeZyy-q2Oz7w'


# start = datetime.timestamp(datetime(2021, 10, 1))
# end = datetime.timestamp(datetime(2022, 2, 3))
# sec = 14400
# market = 'BTC/USD'

# def getData(api, apisecret, market, sec, start, end):
#     try:
#         ts = int(time.time() * 1000)
#         ## add another line here to test get request as 200 response
#         request = Request('GET', f'https://ftx.com/api/markets/{market}/candles?resolution={sec}&start_time={start}&end_time={end}')
#         prepared = request.prepare()
#         signature_payload = f'{ts}{prepared.method}{prepared.path_url}'.encode()
#         signature = hmac.new(apisecret.encode(), signature_payload, 'sha256').hexdigest()
#     except Exception:
#         # usually connection errors here
#         logging.DEBUG('Get request has error')

#     prepared.headers['FTX-KEY'] = api
#     prepared.headers['FTX-SIGN'] = signature
#     prepared.headers['FTX-TS'] = str(ts)
    
#     response = Session().send(prepared)
#     response = response.json()['result']
    
#     return pd.DataFrame(response)

# def hdfWrite(df):
#     # once getData has retrieved historical data for selected coins,
#     # we write to a hdf5 file
    
    
# # do not run main script first
# if __name__ == "__main__": hdfWrite(getData(<params>))