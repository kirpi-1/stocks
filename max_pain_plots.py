import pandas as pd
import yfinance as yf
import sqlite3
import re
import datetime
import numpy as np
import matplotlib.pyplot as plt

def getPuts(a):
	return re.match('\\w*[0-9]{6}C.*', a)!=None

def getCalls(a):
	return re.match('\\w*[0-9]{6}C.*', a)!=None

def getDates(a):
	raw = re.search('[0-9]{6}', a)[0]
	year = int(raw[0:2])+2000
	month = int(raw[2:4])
	day = int(raw[4:])
	date = datetime.datetime(year=year, month=month, day=day)
	return date


def get_option_chain_values(calls, puts, strike_info:dict):
	strikes = np.arange(strike_info['start'],strike_info['stop'], strike_info['inc'])
	strikes = np.expand_dims(strikes, axis=0)
	values = dict()
	for data, name in zip([calls, puts], ['calls', 'puts']):
		if len(data)==0:
			print(f"{name} data is empty")
			return None
		strike_mat = np.repeat(strikes, len(data), axis=0)
		iv = data[['strike']].to_numpy() - strike_mat
		if name=='calls':
			iv = iv*-1
		iv[iv<0] = 0
		values[name] = np.sum(data[['openInterest']].to_numpy() * iv * 100, axis=0)
	values['strikes'] = np.squeeze(strikes)
	return values

conn = sqlite3.connect('options.db')
ticker = 'GME'
ticker_data = pd.read_sql(f"SELECT * FROM options WHERE contractSymbol LIKE '{ticker}%'", conn)
symbol = ticker_data['contractSymbol']
strike_dates = ticker_data['contractSymbol'].apply(getDates)
unique_dates = strike_dates.unique()
# pick a strike date to analyze
max_pain_vals = pd.DataFrame(columns=['strike date', 'max pain'])
for strike_date in unique_dates:
	data = ticker_data[strike_dates==strike_date]
	# for each day we downloaded data, calculate max pain for these contracts
	date_collected = ticker_data.timestamp.unique()
	for dc in date_collected:
		subset = data[data['timestamp']==dc]

		put_idx = subset['contractSymbol'].apply(getPuts).to_numpy()
		call_idx = subset['contractSymbol'].apply(getCalls).to_numpy()
		strike_info={'start':5, 'stop':300, 'inc':5}
		values = get_option_chain_values(subset[call_idx], subset[put_idx], strike_info)
		if values is not None:
			max_pain = values['strikes'][np.argmin(values['calls'] + values['puts'])]
			max_pain_val = np.min(values['calls'] + values['puts'])
		else:
			max_pain = None
		print(strike_date, dc, len(subset), np.sum(subset['openInterest']), max_pain)
		tmp = pd.DataFrame({'strike date':[strike_date], 'max pain':[max_pain]})
		max_pain_vals = pd.concat([max_pain_vals,tmp], ignore_index=True)


plt.figure()
plt.plot(values['strikes'], values['calls'])
plt.plot(values['strikes'], values['puts'])
plt.legend(['calls', 'puts'])
values['strikes'].shape


