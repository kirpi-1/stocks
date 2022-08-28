# a helper function for calculate_distributions.py, calling it with many different dates

import subprocess
import os
import argparse
from datetime import datetime
import logging

logger = logging.getLogger("main")
logger.setLevel(logging.INFO)
fh = logging.FileHandler("log.log")
fh.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter) 
logger.addHandler(fh)


parser = argparse.ArgumentParser()
parser.add_argument("ticker", type=str, default="GME", help="The ticker we're interested in (GME)")
parser.add_argument("datatype", type=str, default="close", help="the data type (folder) you want to use")
parser.add_argument("window", type=int, default=10, help="the rolling window size to use")

args = parser.parse_args()
ticker = args.ticker
data_type = args.datatype
window_size = args.window

for increment in [12]:#, 6, 3]:
    start_date = datetime(year=2018, month=1, day=1)    
    while start_date.year<=2022:
        year = start_date.year
        month = start_date.month+increment
        if month>12:
            month=month%12
            year+=1
        stop_date = datetime(year=year, month=month, day=start_date.day)
        s = " ".join(['python','calculate_distributions.py', ticker, data_type, str(window_size), \
            '--start',str(start_date.day), str(start_date.month), str(start_date.year), \
            '--stop', str(stop_date.day), str(stop_date.month), str(stop_date.year)])
        logger.info(s)
        subprocess.run(s)
        start_date = datetime(year=stop_date.year, month=stop_date.month, day=stop_date.day)
