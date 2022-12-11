# Examining / exporting data from the DB

The app `parfait-list` reads tickers and prices from the database and prints
them to the standard output either as text for easy reading on the terminal, or
in a CSV format:

```sh
parfait-list -db <DB> -tickers [ -csv ]
parfait-list -db <DB> -prices <TICKER> [ -csv ]     # daily prices
parfait-list -db <DB> -monthly <TICKER> [ -csv ]  # monthly prices
```

The CSV format for tickers and daily prices can be imported by the
`parfait-import` command. Thus, these two apps in tandem provide a way to edit
the DB data through editing the CSV files before re-imporiting.
