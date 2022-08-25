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
parser.add_argument("input_file", type=str, help="The distributions/global scores file to serve as input")

#parser.add_argument("--start", nargs=3, type=int, default=[1, 1, 2021],help="Start date. Use three numbers, day month year, with months and days starting at 1. For example --start 15 3 2020 is the 15th of March, 2020")
#parser.add_argument("--stop", nargs=3, type=int, default=[1,1,2022], help="Stop date. Use three numbers, day month year, with months and days starting at 1. For example --start 27 6 2021 is the 27th of June, 2021")
parser.add_argument("--use-smoothing", "-s", type=bool, default=True, help="Whether or not to smooth out sample distributions")
parser.add_argument("--smoothing-window", "-w", type=int, default=51, help="Size of the smoothing window. Must be odd")
parser.add_argument("--smoothing-order", "-r", type=int, default=1, help="The order of the savitzky-golay filter used in smoothing")
parser.add_argument("--overwrite", "-o", type=bool, default=False, help="If there are already existing plots, decide whether or not to overwrite them")

args = parser.parse_args()

if args.use_smoothing and args.smoothing_window%2==0:
    raise Exception("Length of smoothing window must be odd-valued")
smoothing_window = args.smoothing_window
smoothing_order = args.smoothing_order
overwrite = args.overwrite

if "global_scores" in args.input_file:
    tmp = args.input_file.replace("global_scores_","")
elif "distributions" in args.input_file:
    tmp = args.input_file.replace("distributions_","")
else:
    raise Exception("Please use a distributions or global_scores csv as input")
tmp = os.path.splitext(os.path.basename(tmp))[0]
ticker = tmp.split("_")[0]
window_size = tmp.split("_")[1]
start = tmp.split("_")[2]
stop = tmp.split("_")[3]
start_date = datetime.datetime.strptime(start, "%b-%d-%Y")
end_date = datetime.datetime.strptime(stop, "%b-%d-%Y")
#start_date = datetime.datetime(day=args.start[0], month=args.start[1], year=args.start[2])
#end_date = datetime.datetime(day=args.stop[0], month=args.stop[1], year=args.stop[2])


# read the table

table = pq.read_table(f'./data/{window_size}/{ticker}.parquet')
df = table.to_pandas()

if start_date is not None:
    start_txt = start_date.strftime("%b-%d-%Y")
else:
    start_txt = "Beginning"
if end_date is not None:
    end_txt = end_date.strftime("%b-%d-%Y")
else:
    end_txt = "Present-Day"


global_res = pd.read_csv(f'distributions/global_scores_{ticker}_{window_size}_{start_txt}_{end_txt}.csv', index_col=0)
distributions = pd.read_csv(f'distributions/distributions_{ticker}_{window_size}_{start_txt}_{end_txt}.csv', index_col='bins')
dist = distributions.to_dict('list')
dist['bins'] = distributions.index.to_numpy()
order = global_res['ks'].sort_values(ascending=False)
out = global_res.loc[order.index]
out = out.drop(ticker)


def plot_group(idx, group_size, title, plot_legend=True):
    output_filename = f"./plots/global/{ticker}/{window_size}/{start_txt}_{end_txt}/{idx}_{idx+group_size-1}.png"
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
        if args.use_smoothing:
            n = savgol_filter(n, smoothing_window, smoothing_order)
        n = n/np.sum(n)        
        plt.plot(dist['bins'], n, linewidth=2)    
        legends.append(f"{i}:ks={out.loc[i,'ks']}, p={out.loc[i,'p']}")
    plt.yticks(ticks=np.arange(0,1,.2), labels=[])
    legends.append("Global")
    plt.plot(dist['bins'], dist['Global'],color='black',linewidth=5)
    
    plt.ylim([-.0025, .02])  
    if plot_legend:
        plt.legend(legends,prop={'size': 14})    

    plt.title(title, fontsize=20)
    os.makedirs(f"./plots/global/{ticker}/{window_size}/{start_txt}_{end_txt}/", exist_ok=True)
    plt.savefig(output_filename, dpi=200, facecolor="white")
    plt.close()    

idx=0
title = "Correlation distributions:"
logger.info(f"Generating plots for {ticker}, window={window_size}, {start_txt} to {end_txt}")
group_size = 5
title = title+" "+start_txt+" to "+end_txt
while idx<len(out):   
    msg = f"{idx} of {len(out)}"
    print(msg, end='\r' )
    if idx%100==0:
        logger.debug(msg)
    plot_group(idx, group_size, title=title, plot_legend=True)
    idx+=group_size    
    print(" "*80, end='\r')

plot_group(0, 50, title=title+": Top 50", plot_legend=False)
plot_group(0, 100, title=title+": Top 100", plot_legend=False)
logger.info("Completed generating plots")
    