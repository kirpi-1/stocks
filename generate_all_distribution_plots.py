# a helper function to run genearate_distribution_plots.py on a bunch of files
# accepts 3 arguments (ticker, datatype, and window) and passes forward the rest (see genearate_distribution_plots.py for more options)

import subprocess
import os
import sys
import logging
import argparse

logger = logging.getLogger("main")
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

argv = sys.argv
print(argv)
parser = argparse.ArgumentParser()
parser = argparse.ArgumentParser()
parser.add_argument("ticker", type=str, default=None, help="The ticker we're interested in (GME)")
parser.add_argument("datatype", type=str, default=None, help="the data type (folder) you want to use")
parser.add_argument("window", type=int, default=None, help="the rolling window size to use")
args = parser.parse_args(argv[1:4])

input_folder = os.path.join(args.datatype,'distributions')

files = os.listdir(input_folder)
for c in [args.ticker, args.datatype, args.window]:
    if c is not None:    
        files = [f for f in files if str(c) in f]

for f in files:
    cmd = ['python','generate_distribution_plots.py', os.path.join(input_folder, f)]
    if len(sys.argv)>4:
        cmd+= sys.argv[4:]
    logger.info(" ".join(cmd))
    subprocess.run(cmd)
