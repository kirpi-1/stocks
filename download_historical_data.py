import logging
import pandas as pd
import yfinance as yf


logger = logging.getLogger()
# download historical data
file = 'ticker_list.csv'
tickers = pd.read_csv(file)
failed_tickers = list()
df = pd.DataFrame()
dfv = pd.DataFrame()
for idx, ticker in enumerate(tickers['Symbol']):
    msg = f"Working on {ticker}, ({idx+1} of {len(tickers)})"
    print(msg, end="\r")
    logger.info(msg)
    d = yf.Ticker(ticker).history(period="max")
    if len(d)>0:        
        df.insert(len(df.columns), ticker, d.Close)
        dfv.insert(len(dfv.columns), ticker, d.Volume)
        df = df.copy() # to stop pandas from complaining about fragmented dataframes
        dfv = dfv.copy()

    print(" "*80, end="\r")

# save progress
df.to_csv("historical_data.csv", index=True)
dfv.to_csv("historical_volume_data.csv", index=True)
print("Done")
logger.info("Done downloading historical data")