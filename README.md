# yf-parqed

Persistent storage of yfinance ticker data in parquet.  It uses rate limiting on the calls to the Yahoo APIs (can be controlled) so as not to trigger any 
usage limits. The aim is to have something you can install via PyPi that you call on the commandline to update your local copy of ticker data.

It can download the actual lists of nasdaq and nyse tickers from [datahub.io](https://datahub.io/) ( specifically  [nasdaq-listed.csv](https://datahub.io/core/nasdaq-listings/_r/-/data/nasdaq-listed.csv) and [nye-listed.csv](https://datahub.io/core/nyse-other-listings/_r/-/data/nyse-listed.csv) ) and download each ticker found in there via yfinance.

Along with which tickers to sync locally you can also control the iinterval or intervals you are interested in.
Each interval has their own folder and in there each ticker will have their own parquet. 

In addition, if any ticker returns no data or you are no longer interested in syncing data for it you have the option of adding them to an exclude list to
simplify the management of the list of tickers to download.

## About the repo

This repo uses [uv](https://docs.astral.sh/uv/) and [pre-commit-uv](https://pypi.org/project/pre-commit-uv/).  

Make sure that uv is installed and then execute `uv tool install pre-commit --with pre-commit-uv --force-reinstall` in the repo.

## How to install

Simply use pip or your favorite package management tool to install the package: `pip install yf-parqed`

## How to use

Details still in flux but generally:

1. Initialize the list of tickers to sync (automatic or manual) 
2. Decide on the intervals of interest
3. Trigger the sync

At any time there after you can trigger a re-sync which will aim to download all data since your last sync based on the 
max dates of each locally synced ticker.




