import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("file", type=str, default='global_scores.csv')
args = parser.parse_args()
global_res = pd.read_csv(args.file, index_col=0)

# plot the distribution of KS scores to visually make sure it matches
# also sort tickers based on KS score
n, bins = np.histogram(global_res['ks'], bins=np.arange(0,1.1,.05))
# this should roughly look like the KS statistic distribution
# (https://en.wikipedia.org/wiki/Kolmogorov%E2%80%93Smirnov_test#/media/File:KolmogorovDistrPDF.png)
plt.plot(bins[0:-1],n)
mngr = plt.get_current_fig_manager()
mngr.window.setGeometry(50,100,640, 545)
plt.title(f"Distribution of KS-statistic for {args.file}")
plt.show(block=True)
