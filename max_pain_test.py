import pandas as pd
import re
import yfinance as yf
import numpy as np
import matplotlib.pyplot as plt
from collections import namedtuple
from get_sectors import get_sectors


# get list of tickers with sectors from https://www.stockmonitor.com/sectors/
# save it to disk as 'stock_monitor_tickers.csv'
tickers = pd.read_csv('stock_monitor_tickers.csv', encoding='utf8', names=['Symbol','Sector'])
# read finra metadata
finra_metadata = pd.read_csv('FINRA_metadata.csv')
base = 'FINRA/'

idx = tickers[tickers['Symbol']=='GME'].index
sector = tickers.iloc[idx.values[0]]['Sector']
consumer_cyclical_idx = tickers['Sector']=='Consumer Cyclical'
consumer_cyclical_symbols = tickers[consumer_cyclical_idx]

ticker = 'GME'
d = yf.Ticker(ticker)
exchange = d.info['exchange']
prefix = ''
if exchange=='NYQ':
    prefix = 'FNYX'

tmp = [a for a in finra_metadata['code'] if re.match(f'.*{prefix}_{ticker}$', a)]
history = d.history(period='max')


# options

option_dates = d.options
chain_data = d.option_chain(option_dates[0])
['strike','openInterest','volume']
#'contractSize' == 'REGULAR'

test_data = np.array([[95,21,9],[100,33,45],[105,5,25]])
df = pd.DataFrame(test_data, columns=['strike','put open interest','call open interest'])

test_data = np.expand_dims(test_data,2)
# make a horizontal axis of all the strike prices we want to calculate for
start_strike = 95
stop_strike = 115
strike_increment = 5
strikes = np.expand_dims(np.arange(start_strike, stop_strike, strike_increment), axis=0)
# make a matrix for faster calculations
strike_mat = np.repeat(strikes,len(df), axis=0)
'''
<- stock price ->
    95 100 105 110
    95 100 105 110
    95 100 105 110
'''

iv = df[['strike']].to_numpy() - strike_mat


#put_iv[iv>0]=iv[iv>0]
#call_iv[iv<0]=-1*iv[iv<0]

put_value = df[['put open interest']].to_numpy()*iv*100
call_value = df[['call open interest']].to_numpy()*iv*100
p = np.sum(put_value, axis=0)
c = np.sum(call_value, axis=0)
value = p+c

def get_option_chain_values(calls, puts, strike_info:dict):
    strikes = np.arange(strike_info['start'],strike_info['stop'], strike_info['inc'])
    strikes = np.expand_dims(strikes, axis=0)
    values = dict()
    for data, name in zip([calls, puts], ['calls', 'puts']):
        strike_mat = np.repeat(strikes, len(data), axis=0)
        iv = data[['strike']].to_numpy() - strike_mat
        if name=='calls':
            iv = iv*-1
        iv[iv<0] = 0
        values[name] = np.sum(data[['openInterest']].to_numpy() * iv * 100, axis=0)
    values['strikes'] = np.squeeze(strikes)
    return values

strike_info = {'start':0, 'stop':305,'inc':5}
vals = get_option_chain_values(chain_data.calls, chain_data.puts, strike_info)
plt.plot(vals['strikes'], vals['calls'], color='green')
plt.plot(vals['strikes'], vals['puts'], color='red')
max_pain = vals['strikes'][np.argmin(vals['calls']+vals['puts'])]
max_pain_val = np.min(vals['calls']+vals['puts'])
ca = plt.gca()
ylim = ca.get_ylim()
plt.text(max_pain,max_pain_val+.20*(ylim[1]-ylim[0]),f"Max Pain = {max_pain}" , horizontalalignment='center')
#plt.vlines(max_pain, ylim[0], min(maxylim[1]/2, color='black')
plt.legend(['Calls', 'Puts', 'Max Pain'])
plt.show()
