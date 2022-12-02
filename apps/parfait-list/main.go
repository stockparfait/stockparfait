// Copyright 2022 Stock Parfait

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//     http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"flag"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/stockparfait/errors"
	"github.com/stockparfait/logging"
	"github.com/stockparfait/stockparfait/db"
	"github.com/stockparfait/stockparfait/table"
)

type Flags struct {
	DBDir    string // default: ~/.stockparfait
	DBName   string // required
	LogLevel logging.Level
	// Exactly one of tickers, prices or monthly must be present.
	Tickers bool
	Prices  string // ticker to print prices for
	Monthly string // ticker to print monthly data for
	CSV     bool   // dump CSV format; default: text.
}

func parseFlags(args []string) (*Flags, error) {
	var flags Flags
	fs := flag.NewFlagSet("parfait-list", flag.ExitOnError)
	fs.StringVar(&flags.DBDir, "cache",
		filepath.Join(os.Getenv("HOME"), ".stockparfait"),
		"path to databases")
	fs.StringVar(&flags.DBName, "db", "", "database name (required)")
	flags.LogLevel = logging.Info
	fs.Var(&flags.LogLevel, "log-level", "Log level: debug, info, warning, error")
	fs.BoolVar(&flags.Tickers, "tickers", false, "print all ticker rows")
	fs.StringVar(&flags.Prices, "prices", "", "ticker to print prices for")
	fs.StringVar(&flags.Monthly, "monthly", "", "ticker to print monthly data for")
	fs.BoolVar(&flags.CSV, "csv", false, "print table in CSV format; default: text")

	err := fs.Parse(args)
	if err != nil {
		return nil, err
	}
	if flags.DBName == "" {
		return nil, errors.Reason("missing required -db argument")
	}
	kinds := 0
	if flags.Tickers {
		kinds++
	}
	if flags.Prices != "" {
		kinds++
	}
	if flags.Monthly != "" {
		kinds++
	}
	if kinds != 1 {
		return nil, errors.Reason(
			"expected exactly one of -tickers, -prices or -monthly")
	}
	return &flags, err
}

func tickersTable(ctx context.Context, reader *db.Reader) (*table.Table, error) {
	tickers, err := reader.Tickers(ctx)
	if err != nil {
		return nil, errors.Annotate(err, "failed to read tickers")
	}
	sort.Slice(tickers, func(i, j int) bool { return tickers[i] < tickers[j] })
	var rows []table.Row
	for _, t := range tickers {
		tr, err := reader.TickerRow(t)
		if err != nil {
			return nil, errors.Annotate(err, "failed to read ticker row for %s", t)
		}
		rows = append(rows, tr.Row(t))
	}
	tbl := table.NewTable(db.TickerRowHeader()...)
	tbl.AddRow(rows...)
	return tbl, nil
}

func pricesTable(ctx context.Context, reader *db.Reader, ticker string) (*table.Table, error) {
	prices, err := reader.Prices(ticker)
	if err != nil {
		return nil, errors.Annotate(err, "failed to read prices for %s", ticker)
	}
	rows := make([]table.Row, len(prices))
	for i, p := range prices {
		rows[i] = p
	}
	tbl := table.NewTable(db.PriceRowHeader()...)
	tbl.AddRow(rows...)
	return tbl, nil
}

func monthlyTable(ctx context.Context, reader *db.Reader, ticker string) (*table.Table, error) {
	monthly, err := reader.Monthly(ticker, db.Date{}, db.Date{})
	if err != nil {
		return nil, errors.Annotate(err, "failed to read monthly data for %s", ticker)
	}
	rows := make([]table.Row, len(monthly))
	for i, m := range monthly {
		rows[i] = m
	}
	tbl := table.NewTable(db.ResampledRowHeader()...)
	tbl.AddRow(rows...)
	return tbl, nil
}

func printData(ctx context.Context, flags *Flags, w io.Writer) error {
	var tbl *table.Table
	var err error
	reader := db.NewReader(flags.DBDir, flags.DBName)
	if flags.Tickers {
		if tbl, err = tickersTable(ctx, reader); err != nil {
			return errors.Annotate(err, "failed to read tickers")
		}
	}
	if flags.Prices != "" {
		if tbl, err = pricesTable(ctx, reader, flags.Prices); err != nil {
			return errors.Annotate(err, "failed to read prices for %s", flags.Prices)
		}
	}
	if flags.Monthly != "" {
		if tbl, err = monthlyTable(ctx, reader, flags.Monthly); err != nil {
			return errors.Annotate(err, "failed to read monthly data for %s",
				flags.Monthly)
		}
	}
	if tbl == nil {
		return errors.Reason("no data")
	}
	if flags.CSV {
		if err := tbl.WriteCSV(w, table.Params{}); err != nil {
			return errors.Annotate(err, "failed to print CSV")
		}
		return nil
	}
	if err := tbl.WriteText(w, table.Params{}); err != nil {
		return errors.Annotate(err, "failed to print text")
	}
	return nil
}

func main() {
	ctx := context.Background()
	flags, err := parseFlags(os.Args[1:])
	if err != nil {
		ctx = logging.Use(ctx, logging.DefaultGoLogger(logging.Info))
		logging.Errorf(ctx, "failed to parse flags: %s", err.Error())
		os.Exit(1)
	}
	ctx = logging.Use(ctx, logging.DefaultGoLogger(flags.LogLevel))

	if err := printData(ctx, flags, os.Stdout); err != nil {
		logging.Errorf(ctx, err.Error())
		os.Exit(1)
	}
}
