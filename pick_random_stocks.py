import pandas as pd
import yfinance as yf
import numpy as np
import os
all_tickers = pd.read_csv('stock_monitor_tickers.csv', encoding='utf8', names=['Symbol','Sector'])

starter_tickers = ['GME', 'GOOG', 'AAPL', 'MSFT', 'WMT', 'TGT', 'BBY', 'BBBY', 'AMC', 'NFLX', 'META', 'AMD',
                   'AMZN', 'BAC', 'CCL', 'RBLX', 'SOFI', 'BABA', 'SPY', 'JPM', 'WFC', 'C', 'CS','TWTR']

# get sectors
sectors = list()
for ticker in starter_tickers:
    idx = all_tickers[all_tickers['Symbol']==ticker].index
    if len(idx>0):
        sectors.append(all_tickers['Sector'][idx].values[0])
sectors = list(set(sectors))

ticker_pool = list()
ticker_pool += starter_tickers
# for each sector, pick out 20 random stocks
for sector in sectors:
    print("Finding", sector, "symbols")
    tmp = all_tickers[all_tickers['Sector']==sector]
    add_list = list()
    while len(add_list)<20:
        idx = np.random.randint(0,len(tmp))
        symbol = tmp['Symbol'].iloc[idx]
        d = yf.Ticker(symbol)
        options_dates = d.options
        if len(options_dates)==0:
            print("No options info found for", symbol, ", skipping")
            continue
        if symbol not in starter_tickers and symbol not in add_list:
            add_list.append(symbol)
    ticker_pool+=add_list
if os.path.exists('ticker_pool.csv'):
    print("Ticker pool already exists, just printing the tickers")
    for t in ticker_pool:
        print(t)
else:
    df = pd.DataFrame(ticker_pool, columns=['Symbol']).to_csv('ticker_pool.csv', index=False)