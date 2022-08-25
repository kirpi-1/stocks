# Run order
1. make_ticker_list.py
2. download_historical_data.py
3. calculate_correlations.py
	- use optional arguments "--window_sizes" and "--start_date"
4. get_global_distributions
	- requires the target ticker, window size
	- optionally include a start and stop date, can use anything
5. (optional) plot_ks_scores.py
6.
	