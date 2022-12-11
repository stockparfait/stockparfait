# Importing data from CSV files

The app `parfait-import` populates the database from external CSV files. This is
useful for importing data downloaded from otherwise unsupported sources, such as
TradingView or Alpha Vantage.

Additionally, it can be used to modify the existing data by first exporting it
with [parfait-list], editing the CSV files and re-importing them back.

CSV files must have a header. The order of columns can be arbitrary, and omitted
columns result in zero values.  The column headers by default are expected to be
as exported by `parfait-list`, and can be customized with a schema config.

## Quick reference

```sh
parfait-import -db DB -tickers file.csv [ -replace ] [ -schema schema.json ]
parfait-import -db DB -prices file.csv -ticker TICKER [ -schema schema.json ]
parfait-import -db DB -update-metadata  # recompute metadata
parfait-import -db DB -cleanup          # delete orphaned price files
```

## Importing Tickers

```sh
parfait-import -tickers file.csv [ -replace ] [ -schema schema.json ]
```

The CSV file should contain one ticker per row, and at the minimum must have the
`Ticker` column or its equivalent in a custom schema. The format of the schema
file is defined by `TickerRowConfig` in [csv.go].

With `-replace` flag, the entire tickers table is replaced by the imported file;
otherwise the imported tickers are merged into the existing table.

As an example, tickers from the [TradingView] stock screener can be imported as
follows:

- In the Screener tab, enable columns `Ticker`, `Sector`, `Industry` and
  `Exchange`;
- Filter the stocks as needed;
- Download the screen (click the download button) as `screener.csv`;
- Create `tickers-schema.json` schema file containing:

```json
{
    "Name": "Description"
}
```

- Run the command:

```sh
parfait-import -db tradingview -tickers screener.csv -schema tickers-schema.json
```

## Importing Prices

```sh
parfait-import -db DB -prices file.csv -ticker TICKER [ -schema schema.json ]
```

This updates both the daily and monthly prices for the given ticker, and the
prices are automatically sorted by date.

The CSV prices file should contain the `Date` and one of the price columns or
their equivalents in a custom schema.

The format of the schema file is defined by `PriceRowConfig` in [csv.go], and
multiple price values can be mapped to the same CSV column. For instance, the
following schema will populate the `Close` field from `unadjusted close` column,
and both split adjusted and fully adjusted prices from the same `close` column:

```json
{
  "Close": "unadjusted close",
  "Close split adj": "close",
  "Close fully adj": "close"
}
```

The best way to import prices of a ticker from [TradingView] is to use a Pine script:

```
//@version=5
indicator("Download", overlay=true)

split_adj_ticker = ticker.new(syminfo.prefix, syminfo.ticker, session.regular, adjustment.splits)
split_adj = request.security(split_adj_ticker, timeframe.period, close, barmerge.gaps_on)
plot(split_adj, title="Close split adj", color=color.green)

unadj_ticker = ticker.new(syminfo.prefix, syminfo.ticker, session.regular, adjustment.none)
unadj = request.security(unadj_ticker, timeframe.period, close, barmerge.gaps_on)
plot(unadj, title="Close", color=color.black)

plot(close, title="Close fully adj", color=color.blue)
plot(close * volume, title="Cash Volume", color=color.red, style=plot.style_histogram, display=display.none)
```

Add the script to the chart, display the ticker of your choice, set the time
interval to `D` (daily), scroll all the way to the left to load the maximum
number of historical data, and export chart data as `TICKER.csv` using `ISO
time` format. This is important, since the default UNIX time format is not
recognized.

Use the custom `prices-schema.json`:

```json
{
  "Date": "time"
}
```

then run the command:

```sh
parfait-import -db tradingview -prices TICKER.csv -schema prices-schema.json
```

## Metadata and cleanup

Although not strictly necessary, it is a good practice to update the metadata
after all the prices are imported:

```sh
parfait-import -db <DB> -update-metadata
```

This reads the entire database and saves various statistics in the
`metadata.json` file in the database folder.

If the tickers table was imported with `-replace`, some price files may have
become orphaned, that is, they are no longer searchable through the DB API, and
can be deleted:

```sh
parfait-import -db <DB> -cleanup
```

[parfait-list]: ../parfait-list
[csv.go]: ../../db/csv.go
[TradingView]: https://www.tradingview.com
