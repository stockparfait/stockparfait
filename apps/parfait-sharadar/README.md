# Downloading data from NASDAQ Data Link / Sharadar

```sh
parfait-sharadar [ -db <DB> ]  # default DB name: "sharadar"
```

This app supports downloading daily data from
[Sharadar US Equities and Fund Prices] to the Stock Parfait database format.
This requires a subscription to the data source.

The app requires `config.toml` file present in the database location, usually in `~/.stockparfait/sharadar/config.toml` containing:

```toml
key = "<<your subscription key>>"
tables = ["SEP", "SFP"]  # keep only the tables you need / subscribed to
```

[Sharadar US Equities and Fund Prices]: https://data.nasdaq.com/databases/SFB/data
