import os
import pandas as pd
import numpy as np
import logging
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from datetime import timedelta
from scipy import stats
from scipy.signal import savgol_filter
import argparse
import re


logger = logging.getLogger()
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)  
logger.addHandler(ch)
logger.setLevel(logging.DEBUG)

parser = argparse.ArgumentParser()
parser.add_argument("ticker", type=str, help="The source ticker you want to correlate everything with")
parser.add_argument("data_type", type=str, default="close", help="The data type you wish to calculate distributions for")
parser.add_argument("window_size", type=int, default=10, help="The rolling window size")
parser.add_argument("--start", nargs=3, type=int, default=[1, 1, 2020],
    help="Start date. Use three numbers, day month year, with months and days starting at 1. For example --start 15 3 2020 is the 15th of March, 2020")
parser.add_argument("--stop", nargs=3, type=int, default=None,
    help="Stop date. Use three numbers, day month year, with months and days starting at 1. For example --start 27 6 2021 is the 27th of June, 2021. Defaults to most recent day with data.")
parser.add_argument("--use-smoothing", "-s", type=bool, default=True, help="Whether or not to smooth out sample distributions")
parser.add_argument("--smoothing-window", "-w", type=int, default=51, help="Size of the smoothing window. Must be odd")
parser.add_argument("--smoothing-order", "-o", type=int, default=1, help="The order of the savitzky-golay filter used in smoothing")

args = parser.parse_args()

if args.use_smoothing and args.smoothing_window%2==0:
    raise Exception("Length of smoothing window must be odd-valued")
smoothing_window = args.smoothing_window
smoothing_order = args.smoothing_order
ticker = args.ticker
data_type = args.data_type
window_size = args.window_size

# read parquet file, convert to dataframe, then select our dates
table = pq.read_table(os.path.join(data_type, str(window_size), f'{ticker}.parquet'))
df = table.to_pandas()
df = df.drop(ticker, axis=1)
start_date = datetime(day=args.start[0], month=args.start[1], year=args.start[2])
if args.stop is None:
    stop_date = df.index[-1]
else:
    stop_date = datetime(day=args.stop[0], month=args.stop[1], year=args.stop[2])
if start_date is not None:
    df = df[df.index>=start_date]
if stop_date is not None:
    df = df[df.index<stop_date]

# remove tickers that have too many null values
na_idx = np.where(df.isna().sum()>0)[0]
tmp = df.isna().sum()
bads = tmp[na_idx]/len(df)>.10
for bad_ticker in bads.index[bads==True]:   
    df = df.drop(bad_ticker, axis=1)
    
# create bins for counting correlation of periods
dist = dict()
dist['bins'] = np.arange(-1.0, 1.01, .01)

# load ticker info, get sectors
ticker_info = pd.read_csv('stock_monitor_tickers.csv')
sectors = ticker_info['Sector'].unique()

# get global distribution 
# combine all the correlation scores and get the histogram of it
# use savitzky-golay filter to smooth it out
global_corr = np.reshape(df.to_numpy(), [-1,1])
global_n, _ = np.histogram(global_corr, bins=dist['bins'])
if args.use_smoothing:
    global_n = savgol_filter(global_n, smoothing_window, smoothing_order)
global_n = global_n/np.sum(global_n)
dist['Global'] = global_n

# get distributions for each sector
ticker_pool = set(df.columns)
for sector in sectors:    
    # get the tickers in this sector
    idx = ticker_info['Sector']==sector
    sector_tickers = set(ticker_info.loc[idx, 'Symbol'])
    # valid tickers are those that are in this sector and that we have data for
    valid_tickers = list(sector_tickers & ticker_pool)
    if ticker in valid_tickers: # don't include yourself in correlations
        valid_tickers.remove(ticker)
    # lump everything together and calculate the histogram, use savgol to smoooth
    sector_corr = np.reshape(df[valid_tickers].to_numpy(), [-1,1])
    sector_n,_ = np.histogram(sector_corr, bins=dist['bins'])
    if args.use_smoothing:
        sector_n = savgol_filter(sector_n, smoothing_window, smoothing_order)
    sector_n = sector_n/np.sum(sector_n)
    dist[sector] = sector_n

# for every symbol, get its correlation distribution
# smooth it out a little with 1st-order savitzky-golay (moving average)
#global_res = pd.DataFrame(columns=['ks','p'], index=df.columns)
#sector_res = pd.DataFrame(columns=['ks','p'], index=df.columns)
results = pd.DataFrame(columns=['global_ks', 'global_p', 'sector_ks', 'sector_p', 'sector', 'pos_score', 'neg_score'], index=df.columns)
# for all stocks symbols
for symbol in df.columns:
    data = df[symbol]
    n, bins = np.histogram(data, dist['bins'])
    if sum(n)==0:
        continue           
    if args.use_smoothing:
        n = savgol_filter(n, smoothing_window, smoothing_order)
    n = n/np.sum(n)
    r = stats.kstest(dist['Global'], n)
    #global_res.loc[symbol, 'ks'] = r.statistic
    #global_res.loc[symbol, 'p'] = r.pvalue
    results.loc[symbol, 'global_ks'] = r.statistic
    results.loc[symbol, 'global_p'] = r.pvalue
    
    # find out which sector this symbol is in
    sector = ticker_info.loc[ticker_info['Symbol']==symbol, 'Sector']
    if len(sector)==0:
        continue
    sector = sector.iloc[0]    
    # compare the correlation of our stock of interest (GME) with
    # this stock against the average of all stocks in the same sector
    r = stats.kstest(dist[sector], n)
    #sector_res.loc[symbol,'ks'] = r.statistic
    #sector_res.loc[symbol,'p'] = r.pvalue
    #sector_res.loc[symbol,'sector'] = sector
    results.loc[symbol, 'sector_ks'] = r.statistic
    results.loc[symbol, 'sector_p'] = r.pvalue
    results.loc[symbol, 'sector'] = sector
    
    #calculate positvie corr and negative corr score
    results.loc[symbol, 'pos_score'] = np.sum(data[data>.5])
    results.loc[symbol, 'neg_score'] = np.sum(data[data<-.5])

# calculate the highly positively and negatively correlated stonks
# normalize by number of days in the period

#positives = df[df>.75].sum().sort_values(ascending=False)
#negatives = df[df<-.75].sum().sort_values(ascending=True)
#positives = positives.to_frame().reset_index()
#positives.columns=['symbol', 'sum']
#negatives = negatives.to_frame().reset_index()
#negatives.columns=['symbol', 'sum']

# save the output
tmp = dist.copy()
tmp['bins'] = tmp['bins'][0:-1]
distributions = pd.DataFrame(tmp)
distributions.index=distributions['bins']
distributions = distributions.drop('bins', axis=1)

start_txt = ""
end_txt = ""
if start_date is not None:
    start_txt = start_date.strftime("%Y-%m-%d")
if stop_date is not None:
    end_txt = stop_date.strftime("%Y-%m-%d")
    
base_output_folder = os.path.join(data_type, "distributions")
os.makedirs(base_output_folder, exist_ok=True)
tag = f"{ticker}_{data_type}_{window_size}_{start_txt}_{end_txt}"
distributions.to_csv(os.path.join(base_output_folder,f'distributions_{tag}.csv'))
#global_res.to_csv(os.path.join(base_output_folder, f'global-scores_{tag}.csv'))
#sector_res.to_csv(os.path.join(base_output_folder, f'sector-scores_{tag}.csv'))
#positives.to_csv(os.path.join(base_output_folder, f'positives_{tag}.csv'))
#negatives.to_csv(os.path.join(base_output_folder, f'negatives_{tag}.csv'))
results.to_csv(os.path.join(base_output_folder, f'results_{tag}.csv'))

logging.info(f"saved scores for {tag}")

