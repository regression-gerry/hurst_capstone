## Structure


## dao -> data connectors
  - connect to ftx
  - main separate -> 1min - 4hrs of top 10 daily traded in volume for ftx

## data 
  - dao_main -> retrieve data from FTX to HDF
  - HDF5
    - Cryto
      - Coins (e.g. btc/eth) 
        - 1min/5mins/15mins/...

## live_data 
  - main to get new data (depending on api limit/strategy)
  - deploy strats (1x/day or 4x/day)
  - if 1x a day -> daily rebalancing strat -> model to calculate allcoations daily for top 10 traded coins

## Strategies/signals
  - ancilliary functions -> minmax/detrend/de-seasonalise
  - data -> store backtest results
  - notebooks -> research purpose for finding alpha

## model deployment
  - live data -> minimise bid-ask spread/slippage 
  - transaction cost from FTX (GET request)

