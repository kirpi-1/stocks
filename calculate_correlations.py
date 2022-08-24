import pandas as pd
import numpy as np
import logging
import datetime
import pyarrow as pa
import pyarrow.parquet as pq
import sys
import os

logger = logging.getLogger()
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)  
logger.addHandler(ch)
logger.setLevel(logging.DEBUG)

# calculate correlations for each rolling window
# pick a starting date for rolling correlations
start_date = datetime.datetime(year=2017, month=1,day=1)
window_sizes = [5, 10, 20, 60, 120]



# read csv from before and convert the datetime to datetime format for easy selection
df = pd.read_csv("historical_data.csv",index_col="Date")
df.index=pd.to_datetime(df.index, format='%Y-%m-%d')

data = df[df.index>start_date]

for window_size in window_sizes:
    msg = f"starting window size {window_size}"
    logger.info(msg)    
    os.makedirs(f'data/{window_size}',exist_ok=True)
    roll = data.rolling(window=window_size)
    res = dict()
    for column in df.columns:
        res[column] = None
    idx = 0
    for r in roll:
        roll_msg = f"roll {idx+1} of {len(df)-window_size}"
        idx+=1        
        d = r.corr() 
        logger.info(roll_msg)
        for col_idx, column in enumerate(d.columns):
            msg = roll_msg + f": {column}, {col_idx+1} of {len(d.columns)}"
            logger.debug(msg)                   
            out = pd.DataFrame(np.expand_dims(d[column].to_numpy(),axis=0),columns=list(data.keys()))
            out['timestamp'] = r.index.max()
            out = out.set_index('timestamp')
            res[column] = pd.concat([res[column], out])            
    for key in res.keys():
        table = pa.Table.from_pandas(res[key], preserve_index=True)
        pq.write_table(table, f'data/{window_size}/{key}.parquet')    

logger.info("Completed calculating correlations")