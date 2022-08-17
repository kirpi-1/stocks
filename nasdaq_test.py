import nasdaqdatalink
import pandas as pd
import re
import yfinance as yf
from get_sectors import get_sectors
import numpy as np
# get list of tickers with sectors from https://www.stockmonitor.com/sectors/
# save it to disk as 'stock_monitor_tickers.csv'
get_sectors()
# read finra metadata
finra_metadata = pd.read_csv('FINRA_metadata.csv')
base = 'FINRA/'

'''
GME, GOOG, AAPL, MSFT, WMT, TGT, BBY, BBBY, AMC, NFLX, META, AMD, YHOO, AMZN, BAC, CCL, RBLX, SOFI, BABA, SPY, JPM, WFC
C, CS
Consumer Cyclical, Technology, 
'''


pd.DataFrame(ticker_pool, columns=['Symbol']).to_csv('ticker_pool_old.csv')
idx = tickers[tickers['Symbol']=='GME'].index
gme_sector = tickers.iloc[idx.values[0]]['Sector']
consumer_cyclical_idx = tickers['Sector']==gme_sector
consumer_cyclical_symbols = tickers[consumer_cyclical_idx]

idx = tickers[tickers['Symbol']==].index
sector = tickers.iloc[idx.values[0]]['Sector']
n = [a for a in tickers['Name'] if 'Walmart' in a]
print(sector)



ticker = 'GME'
d = yf.Ticker(ticker)
exchange = d.info['exchange']
prefix = ''
if exchange=='NYQ':
    prefix = 'FNYX'

tmp = [a for a in finra_metadata['code'] if re.match(f'.*{prefix}_{ticker}$', a)]
code = base+tmp[0]
data = nasdaqdatalink.get(code)
history = d.history(period='max')

dateShortInterest : 1648684800
shortPercentOfFloat : 0.0156
sharesShortPriorMonth : 1997264
sharesOutstanding : 209136992
sharesShort : 2330668
sharesShortPreviousMonthDate : 1646006400
shortRatio : 2.1
averageVolume : 1222493
impliedSharesOutstanding : 0
sharesPercentSharesOut : 0.0111
totalCashPerShare : 15.152 # = totalCash/sharesOutstanding
totalCash : 3168880896
