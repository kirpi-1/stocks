# Run order
1. make_ticker_list.py
2. download_historical_data.py
3. calculate_correlations.py
	- use optional arguments "--window_sizes" and "--start_date"
4. calculate_distributions
	- requires the target ticker, window size
	- optionally include a start and stop date, can use anything
	- has a helper function (get_distributions)
5. (optional) plot_ks_scores.py
6. generate_distribution_plots
	- has a helper function (generate_all_distribution_plots)
	