# Stock Screener

The app `parfait-screener` selects stocks based on the user-defined criteria and
prints them out on `stdout` as a table either in text or CSV format, one ticker
per row. The columns of the table are configurable, see `Config` type in the [`screener/schema.go`](../../screener/schema.go) file for the  configuration schema.

Examples:

File `config.json`:

```js
{
  "columns": [
    {"kind": "ticker", "sort": "ascending"},
    {"date": "2023-03-08", "kind": "price"}
  ],
  "data": {
    "DB": "sharadar",
    "active": true,
    "cash volume": {"min": 10000000},
    "start": "2020-01-01"
  }
}
```

Run the app:

```sh
parfait-screener -conf config.json -csv > stocks.csv
```
