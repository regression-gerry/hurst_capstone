import time
import hmac
from requests import Request, Session
import pandas as pd
import numpy as np
import logging
import sys
sys.path.extend(['../_crypto'])

logging.basicConfig(
    level=logging.DEBUG,
    format='{asctime} {levelname: <8} {message}',
    style='{'
    handlers=[
        logging.FileHandler("./logs/data_connector_log.log"),
        logging.StreamHandler(sys.stdout)
    ],
    force=True
)

## under testing
api = 'qSt6XBJujWQmfOB0hFp0JX7nVM_tqz4iZJ_wR57m'
apisecret = 'OYBcxl2FQ_eDboeX7B31owUB8afgLeZyy-q2Oz7w'

from datetime import datetime
start = datetime.timestamp(datetime(2021, 10, 1))
end = datetime.timestamp(datetime(2022, 2, 3))
sec = 14400
market = 'BTC/USD'

def getData(api, apisecret, market, sec, start, end):
    try:
        ts = int(time.time() * 1000)
        ## add another line here to test get request as 200 response
        request = Request('GET', f'https://ftx.com/api/markets/{market}/candles?resolution={sec}&start_time={start}&end_time={end}')
        prepared = request.prepare()
        signature_payload = f'{ts}{prepared.method}{prepared.path_url}'.encode()
        signature = hmac.new(apisecret.encode(), signature_payload, 'sha256').hexdigest()
    except Exception:
        # usually connection errors here
        logging.DEBUG('Get request has error')

    prepared.headers['FTX-KEY'] = api
    prepared.headers['FTX-SIGN'] = signature
    prepared.headers['FTX-TS'] = str(ts)
    
    response = Session().send(prepared)
    response = response.json()['result']
    
    return pd.DataFrame(response)

def hdfWrite(df):
    # once getData has retrieved historical data for selected coins,
    # we write to a hdf5 file
    
    
# do not run main script first
if __name__ == "__main__": hdfWrite(getData(<params>))