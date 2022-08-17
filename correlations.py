import pandas as pd
import yfinance as yf
import sqlite3
import re
import datetime
import numpy as np
import matplotlib.pyplot as plt
import pickle
import logging
logger = logging.getLogger()
# get historical data
files = ['ticker_pool.csv', 'russel1000.csv', 'SP500.csv']
for file in files:
	tickers = pd.read_csv(file)
	#conn = sqlite3.connect('options.db')
	#timestamp = datetime.datetime.now()
	failed_tickers = list()
	data = dict()
	for idx, ticker in enumerate(tickers['Symbol']):
		msg = f"Working on {ticker}, ({idx+1} of {len(tickers)})"
		print(msg)
		logger.info(msg)
		data[ticker] = yf.Ticker(ticker).history(period="max")

	with open(f'historical_data_{file.split(".")[0]}.pkl', 'wb') as f:
		pickle.dump(data,f)


# Part 2
# correlate data and save
with open('historical_data.pkl', 'rb') as f:
	data = pickle.load(f)

date = datetime.datetime(year=2017, month=1,day=1)
df = pd.DataFrame(columns=list(data.keys()))
for key in data.keys():
	df[key] = data[key][data[key].index>date]['Close']
#df[t2] = data[t2][data[t2].index>date]['Close']
window_size = 10

for window_size in [5, 10, 20, 120]:
	roll = df.rolling(window=window_size)
	res = {}
	for column in df.columns:
		res[column] = None
	idx = 0
	for r in roll:
		msg = f"roll {idx}"
		idx+=1
		logger.info(msg)
		print(msg)
		d = r.corr()
		for column in d.columns:
			out = pd.DataFrame(np.expand_dims(d[column].to_numpy(),axis=0),columns=list(data.keys()))
			out['timestamp'] = r.index.max()
			out = out.set_index('timestamp')
			res[column] = pd.concat([res[column], out])

	with open(f'correlations_{window_size}_days.pkl', 'wb') as f:
		pickle.dump(res, f)


# part 3
# plot data

window_size = 10

with open(f'correlations_{window_size}_days.pkl', 'rb') as f:
	res = pickle.load(f)

target='GME'
tickers = ['BBBY','GOOG','TWTR','AMC','AMZN','BB']
plt.figure()
plt.plot(res[target].index, res[target][tickers])
plt.legend(tickers)


plt.figure()
idx = 1
colors = ['blue', 'green', 'pink', 'red', 'yellow', 'black']
for ticker in tickers:
	plt.subplot(2,3,idx)
	plt.hist(res[target][ticker], bins=10, color=colors[idx-1])
	plt.legend(ticker)
	idx+=1

plt.figure()
plt.hist(res['GOOG'], bins=100)


# ci
# correltiion distribution across whole stock market
# correlation distribution within a stock sector
