# yf_parqed

Persistent storage of yfinance ticker data in parquet.  It uses rate limiting on the calls to the Yahoo APIs (can be controlled) so as not to trigger any 
usage limits. The aim is to have something you can install via PyPi that you call on the commandline to update your local copy of ticker data.

## About the repo

This repo uses [uv](https://docs.astral.sh/uv/) and [pre-commit-uv](https://pypi.org/project/pre-commit-uv/).  

Make sure that uv is installed and then execute `uv tool install pre-commit --with pre-commit-uv --force-reinstall` in the repo.



## Pre-flight acttivities

Make sure you go to [datahub.io](https://datahub.io/) and download an up to date version of [nasdaq-listed.csv](https://datahub.io/core/nasdaq-listings/_r/-/data/nasdaq-listed.csv) and [nye-listed.csv](https://datahub.io/core/nyse-other-listings/_r/-/data/nyse-listed.csv) and save them in the `data` folder.  Or go to their respective github repos, clone them and trigger a rebuild from scratch.

If you want to use your up to date lists of nasdaq and nyse listings make sure to first 
