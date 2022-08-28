import pandas as pd
import numpy as np
import datetime
import pyarrow as pa
import pyarrow.parquet as pq
from scipy import stats
from scipy.signal import savgol_filter
import logging
import argparse
from matplotlib import pyplot as plt
import os
import re
logger = logging.getLogger("main")
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)

fh = logging.FileHandler("log.log")
fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)
logger.addHandler(ch)
logger.addHandler(fh)
logger.setLevel(logging.DEBUG)

# argument handling

parser = argparse.ArgumentParser()
#parser.add_argument("ticker", type=str, help="The source ticker you want to correlate everything with")
#parser.add_argument("window_size", type=int, help="The length of the rolling window of the data you want to use")
parser.add_argument("input_file", type=str, help="The distributions file to serve as input")

#parser.add_argument("--start", nargs=3, type=int, default=[1, 1, 2021],help="Start date. Use three numbers, day month year, with months and days starting at 1. For example --start 15 3 2020 is the 15th of March, 2020")
#parser.add_argument("--stop", nargs=3, type=int, default=[1,1,2022], help="Stop date. Use three numbers, day month year, with months and days starting at 1. For example --start 27 6 2021 is the 27th of June, 2021")
parser.add_argument("--no-smoothing", "-s", action='store_true', default=False, help="Turns off smoothing of distributions")
parser.add_argument("--smoothing-window", "-w", type=int, default=51, help="Size of the smoothing window. Must be odd")
parser.add_argument("--smoothing-order", "-r", type=int, default=1, help="The order of the savitzky-golay filter used in smoothing")
parser.add_argument("--overwrite", "-o", action="store_true", default=False, help="If there are already existing plots, decide whether or not to overwrite them")
parser.add_argument("--error-bars", "-e", action="store_true", default=False, help="If you want error bars (+-1 stdev) for the global line")
args = parser.parse_args()

overwrite = args.overwrite
input_file = args.input_file

if "distributions" not in args.input_file:    
    raise Exception("Please use a 'distributions' csv file as input")
tmp = os.path.splitext(os.path.basename(input_file))[0].split("_")
ticker = tmp[1]
data_type = tmp[2]
window_size = tmp[3]
start = tmp[4]
stop = tmp[5]
start_date = datetime.datetime.strptime(start, "%Y-%m-%d")
stop_date = datetime.datetime.strptime(stop, "%Y-%m-%d")
smoothing = not args.no_smoothing

if smoothing and args.smoothing_window%2==0:
    raise Exception("Length of smoothing window must be odd-valued")
smoothing_window = args.smoothing_window
smoothing_order = args.smoothing_order



# read the data
table = pq.read_table(os.path.join(data_type, str(window_size), f'{ticker}.parquet'))
df = table.to_pandas()
df = df[df.index>=start_date]
df = df[df.index<stop_date]
    
base_folder = os.path.join(data_type, "distributions")
results = pd.read_csv(os.path.join(base_folder,f'results_{ticker}_{data_type}_{window_size}_{start}_{stop}.csv'), index_col=0)
#global_res = pd.read_csv(os.path.join(base_folder,f'global-scores_{ticker}_{data_type}_{window_size}_{start}_{stop}.csv'), index_col=0)
#sector_res = pd.read_csv(os.path.join(base_folder, f'sector-scores_{ticker}_{data_type}_{window_size}_{start}_{stop}.csv'), index_col=0)
distributions = pd.read_csv(os.path.join(base_folder,f'distributions_{ticker}_{data_type}_{window_size}_{start}_{stop}.csv'), index_col='bins')

dist = distributions.to_dict('list')
dist['bins'] = distributions.index.to_numpy()

# do global plots
order = results['global_ks'].sort_values(ascending=False)
out = results.loc[order.index]
#out = out.drop(ticker)

# get +-1std dev for global distribution
all_hists = np.zeros([len(dist['bins']), len(df.columns)])
for idx, column in enumerate(df.columns):
    n, bins = np.histogram(df[column], bins=np.arange(-1,1.01,.01))
    n = savgol_filter(n, smoothing_window, smoothing_order)
    n = n/np.sum(n)  
    all_hists[:,idx] = n

mean_hist = np.nanmean(all_hists, axis=1)
std_hist = np.nanstd(all_hists, axis=1)
err = .95*std_hist#/all_hists.shape[1]

def plot_group(idx, group_size, title, plot_legend=True):
    output_filename = os.path.join(data_type, "plots", "global", ticker, str(window_size), f"{start}_{stop}", f"{idx}_{idx+group_size-1}.png")
    if os.path.exists(output_filename) and not overwrite:
        logger.warning(f"File exists for {output_filename} with overwrite=False, skipping...")
        return
    legends = list()    
    figure = plt.figure(figsize=(16,9))   

    if idx+5>len(out):
        ticker_list = list(out.index[idx:-1])        
    else:
        ticker_list = list(out.index[idx:idx+group_size])    

    for i in ticker_list:        
        n,_ = np.histogram(df[i], bins=np.arange(-1,1.01,.01))
        if smoothing:
            n = savgol_filter(n, smoothing_window, smoothing_order)
        n = n/np.sum(n)        
        plt.plot(dist['bins'], n, linewidth=2)    
        legends.append(f"{i}:ks={out.loc[i,'global_ks']}, p={out.loc[i,'global_p']}")
    plt.yticks(ticks=np.arange(0,1,.2), labels=[])      
    
    # plot confidence interval for global
    
    legends.append("Global")
    plt.plot(dist['bins'], dist['Global'],color='black',linewidth=5)
    if args.error_bars:
        plt.fill_between(dist['bins'], dist['Global']-err, dist['Global']+err, color='grey', alpha=.2)

    
    plt.ylim([-.0025, .02])  
    if plot_legend:
        plt.legend(legends,prop={'size': 14})    
    plt.xlabel("Correlation (r)", fontsize=20)
    plt.ylabel("Weight", fontsize=20)

    plt.title(title, fontsize=20)
    os.makedirs(os.path.dirname(output_filename), exist_ok=True)
    plt.savefig(output_filename, dpi=200, facecolor="white")
    plt.close()    

idx=0
title = f"Correlation distributions for {data_type}:"
logger.info(f"Generating plots for {ticker}, window={window_size}, {start} to {stop}")
group_size = 5
title = title+" "+start+" to "+stop
while idx<len(out):   
    msg = f"{idx} of {len(out)}"
    print(msg, end='\r' )
    if idx%100==0:
        logger.debug(msg)
    plot_group(idx, group_size, title=title, plot_legend=True)
    idx+=group_size    
    print(" "*80, end='\r')

# plot top 50 and top 100 
plot_group(0, 50, title=title+": Top 50", plot_legend=False)
plot_group(0, 100, title=title+": Top 100", plot_legend=False)
logger.info("Completed generating plots")
    