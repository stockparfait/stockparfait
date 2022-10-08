# Stock Parfait

[![Build Status](https://github.com/stockparfait/stockparfait/workflows/Tests/badge.svg)](https://github.com/stockparfait/stockparfait/actions?query=workflow%3ATests)
[![GoDoc](https://godoc.org/github.com/stockparfait/stockparfait?status.svg)](http://godoc.org/github.com/stockparfait/stockparfait)


The command line public version of the core engine of Stock Parfait, a stock
screener and an upcoming Monte Carlo simulator for stock prices.

This repository is primarily a collection of libraries for acquiring data and
building custom analyses.

The main purpose of this project is to provide libraries and tools for those
interested in analyzing stocks, and being able to research common investor
questions themselves with numbers and sound statistics.

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
- `parfaitlist` - listing raw data from the downloaded DB, primarily for debugging.

## Quick start

- Subscribe to the data source of your interest on Nasdaq Data Link (currently,
  only Sharadar US Equities and/or Fund Prices are supported);
- Create `~/.stockparfait/sharadar/config.toml` file containing:

```toml
key = "<<your subscription key>>"
tables = ["SEP", "SFP"]  # keep only the tables you need / subscribed to
```

- Install the `sharadar` app (see Installation above) and run it; it should
  download all the data locally (about 1.5GB for both tables).
- Follow instructions in the [experiments] repository to run some prebuilt
  experiments, or write your own. Use the [experiments] repository as a template
  for your implementation.

## Related Projects

- [experiments] hosts various mostly one-off statistical studies and experiments
  with daily stock prices. Some experiments may serve as prototypes for the
  production features in this repository.

## Contributing to Stock Parfait

Pull requests are welcome. We suggest to contact us beforehand to coordinate
your code contributions.

In particular, this repository is considered "production code", and it is a good
practice to prototype new features first in the [experiments] repository.

[experiments]: https://github.com/stockparfait/experiments
