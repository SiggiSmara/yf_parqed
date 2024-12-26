# yf-parqed

Persistent storage of yfinance ticker data in parquet based on [ranaroussi's](https://github.com/ranaroussi) [yfinace](https://ranaroussi.github.io/yfinance/index.html). Primary use case for using this package is collecting data for back testing or historical analysis. If the update interval is less than one day (24 h) this package will skip updating.

It uses rate limiting on the calls to the Yahoo APIs (can be controlled) so as not to trigger any
usage limits.

It can download the actual lists of Nasdaq and Nyse tickers from [datahub.io](https://datahub.io/) ( specifically  [nasdaq-listed.csv](https://datahub.io/core/nasdaq-listings/_r/-/data/nasdaq-listed.csv) and [nye-listed.csv](https://datahub.io/core/nyse-other-listings/_r/-/data/nyse-listed.csv) ) and download historical ticker prices for each ticker found in there via yfinance.

Along with which tickers to sync locally you can also control the time interval(s) you are interested in.
Each interval has their own folder and in there each ticker will have their own parquet.

In addition, if any ticker returns no data or you are no longer interested in syncing data for it you have the option of adding them to an exclude list to simplify the management of the list of tickers to download.

## How to install

Simply use pip or your favorite package management tool to install the package: `pip install yf-parqed`

## How to use

Still in flux, but generally:

1. Initialize the list of tickers to sync via `yf-parqed initialize`
2. Adjust the content of the `intervals,json`, `current_tickers.json` and `not_found_tickers.json`:
   1. intervals.json contains a list of the yfinance intervals you want to download
   2. current_ticker.json contains the list of tickers you want to download
   3. not_found_tickers.json contains a list of tickers that should be excluded
3. Trigger the initial snapshot via `uv-parqed update` with the `--start-date`and `--end-date` parameters set.
4. Any time after that you can run `uv-parqed update` without parameters to add new data to your local snapshot.

### Notes on `update`

 `uv-parqed update`  will detect if any tickers are not returning data and asks you via a prompt if you want to save them to the exclude list. You have two CLI options that allow you to control that behavior in the case when you are running this command via scripts:

- `--save-not-founds` will circumvent the prompt and save them to the exclude list
- `--force-no-save` will circumvent the prompt in the case when the `--save-not-found` is not present (resulting in the exclude list not being updated)

The current list of tickers from Nasdaq and Nyse  (> 9000 tickers in total) with the default limiter settings will take a considerable time given that the default limiter settings is no more than 2 API calls in a 5 second period.  You can of course play around with those settings, but they are coming from the [documentation of yfinance](https://ranaroussi.github.io/yfinance/advanced/caching.html) and they are very stable in my experience.

As the Yahoo finance APIs are rate limited (and not volume) it makes less sense (to me at least) to use an API cache mechanism, although that is easy to set up as well (see the above link to the yfinance documentation).

## About the package

This repo uses [uv](https://docs.astral.sh/uv/) and [pre-commit-uv](https://pypi.org/project/pre-commit-uv/).  

Make sure that uv is installed and then execute `uv tool install pre-commit --with pre-commit-uv --force-reinstall` in the repo.

The package is created with the [typer module](https://typer.tiangolo.com/) from [tiangolo](https://github.com/tiangolo),
so you can always add `--help` at the end of your cli command to get more information about options
and functionalities.

Logging is taken care of via loguru, and via the `--log-level` option you have access to set the level of logging detail.  Default logging level is `INFO`, for more verbose output set it to `DEBUG`.
