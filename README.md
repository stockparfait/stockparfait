# Stock Parfait

The command line public version of the core engine of Stock Parfait, a stock
screener and an upcoming Monte Carlo simulator for stock prices.

This repo is primarily a collection of libraries for acquiring data and building
custom analyses.

The main purpose of this project is to provide libraries and tools for those
interested in analyzing stocks, and being able to answer common investor
questions for yourself with numbers and sound statistics.

## Installation

Requirements:
- [Google Go](https://go.dev/dl/) 1.16 or higher

```
git clone https://github.com/stockparfait/stockparfait.git
cd stockparfait
make init
make install
```

This installs all the command line apps in your `${GOPATH}/bin` (run `go env
GOPATH` to find out where your `GOPATH` is). The apps currently include:

- `sharadar` - downloading financial data from Nasdaq Data Link [Sharadar
  US Equities and Fund Prices](https://data.nasdaq.com/databases/SFB/data) data
  (requires subscription).

## Quick start

- Subscribe to the data source of your interest on Nasdaq Data Link (currently,
  only Sharadar US Equities and/or Fund Prices are supported);
- Create `~/.stockparfait/sharadar/config.toml` file containing:

```toml
key = "<<your key>>"
tables = ["SEP", "SFP"]  # keep only the tables you need / subscribed to
```

- Install the `sharadar` app (see Installation above) and run it; it should
  download all the data locally (about 1.5GB for both tables).
- Follow instructions in the
  [stockparfait/experiments](https://github.com/stockparfait/experiments) repo
  to run some prebuilt experiments, or write your own. Use the `experiments`
  repo as a template for your implementation.

## Related Projects

- [stockparfait/experiments](https://github.com/stockparfait/experiments) hosts
  various mostly one-off statistical studies and experiments with daily stock
  prices.
