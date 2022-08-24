import pandas as pd

# make the ticker list
# "custom_ticker_pool.csv" is a list of tickers that you want to include specifically
# even if they're not included in the Russell 1000 or S&P 500
files = ['custom_ticker_pool.csv', 'russell1000.csv', 'SP500.csv']
tickers = list()
for file in files:
    df = pd.read_csv(file)
    tickers+=(df['Symbol'].to_list())
tickers = list(set(tickers))
for t in ["BF.A", "BF.B", "EHAB"]: # remove known bad tickers
    tickers.remove(t)
df = pd.DataFrame(tickers, columns=['Symbol'])
df.to_csv('ticker_list.csv', index=False)