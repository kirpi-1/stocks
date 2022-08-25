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

logger = logging.getLogger()
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)  
logger.addHandler(ch)
logger.setLevel(logging.DEBUG)


window_size = 10
ticker="GME"

table = pq.read_table(f'./data/{window_size}/{ticker}.parquet')
df = table.to_pandas()


start_date = datetime.datetime(day=1, month=1,year=2021)
end_date = datetime.datetime(day=1, month=1, year=2022)

if start_date is not None:
    start_txt = start_date.strftime("%b-%d-%Y")
else:
    start_txt = "Beginning"
if end_date is not None:
    end_txt = end_date.strftime("%b-%d-%Y")
else:
    end_txt = "Present-Day"


global_res = pd.read_csv(f'global_scores_{window_size}_{start_txt}_{end_txt}.csv', index_col=0)
distributions = pd.read_csv(f'distributions_{window_size}_{start_txt}_{end_txt}.csv', index_col='bins')
dist = distributions.to_dict('list')
dist['bins'] = distributions.index.to_numpy()
order = global_res['ks'].sort_values(ascending=False)
out = global_res.loc[order.index]
out = out.drop(ticker)

idx=0
title = "Correlation distributions:"
logger.info(f"Generating plots for window={window_size}, {start_txt} to {end_txt}")
while idx<len(out):
    print(f"{idx} of {len(out)}", end='\r' )
    legends = list()
    legends.append("Global")
    figure = plt.figure(figsize=(16,9))
    plt.plot(dist['bins'], dist['Global'],color='black',linewidth=5)

    if idx+5>len(out):
        ticker_list = list(out.index[idx:-1])        
    else:
        ticker_list = list(out.index[idx:idx+5])    

    for i in ticker_list:        
        n,_ = np.histogram(df[i], bins=np.arange(-1,1.01,.01))
        n = n/np.sum(n)
        n = savgol_filter(n, 51, 1)
        plt.plot(dist['bins'], n, linewidth=2)    
        legends.append(f"{i}:ks={out.loc[i,'ks']}, p={out.loc[i,'p']}")

    plt.ylim([-.0025, .02])
    plt.legend(legends,prop={'size': 14})
    

    plt.title(title+" "+start_txt+" to "+end_txt, fontsize=20)
    os.makedirs(f"./plots/global/{window_size}/{start_txt}_{end_txt}/", exist_ok=True)
    plt.savefig(f"./plots/global/{window_size}/{start_txt}_{end_txt}/{idx}_{idx+4}.png", dpi=200, facecolor="white")
    plt.close()
    idx+=5
    print(" "*80, end='\r')