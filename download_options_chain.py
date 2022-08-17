import pandas as pd
import yfinance as yf
import numpy as np
import sqlite3
import datetime
import logging
import time

logging.basicConfig(filename='log.log')
logger = logging.getLogger('main')
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())

tickers = pd.read_csv('ticker_pool.csv')
conn = sqlite3.connect('options.db')
timestamp = datetime.datetime.now()
df = pd.DataFrame()
failed_tickers = list()
for idx, ticker in enumerate(tickers['Symbol']):
	time.sleep(1)
	logger.info(f"Working on {ticker}, ({idx+1} of {len(tickers)})")
	d = yf.Ticker(ticker)
	retry = True
	count=0
	df_temp = pd.DataFrame()
	while retry and count<5:
		retry = False
		try:
			df_temp = pd.DataFrame()
			option_dates = d.options
			if len(option_dates) == 0:
				logger.error(f"{ticker} has no option dates")
				retry = True
				count += 1
			else:
				for jdx, date in enumerate(option_dates):
					logger.info(f"\t{date}, {jdx+1} of {len(option_dates)}")
					chain_data = d.option_chain(date)
					time.sleep(.1)
					for data in [chain_data.calls, chain_data.puts]:
						data = data.drop(['inTheMoney', 'currency'], axis=1)
						data['timestamp']=timestamp
						#data.to_sql('options', conn, if_exists='append', index=False)
						df_temp = pd.concat([df_temp, data], axis=0)
		except Exception as e:
			logger.error(f"Encountered {e}, retrying")
			retry = True
			count += 1
	df = pd.concat([df, df_temp])
	if count == 5:
		failed_tickers.append(ticker)


df.to_sql('options', conn, if_exists='append', index=False)
df2 = pd.DataFrame(failed_tickers)




