// Copyright 2023 Stock Parfait

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
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"

	"github.com/stockparfait/errors"
	"github.com/stockparfait/iterator"
	"github.com/stockparfait/logging"
	"github.com/stockparfait/stockparfait/db"
	"github.com/stockparfait/stockparfait/message"
	"github.com/stockparfait/stockparfait/table"
)

type Column struct {
	Kind string  `json:"kind" required:"true" choices:"ticker,name,exchange,category,sector,industry,price,volume"`
	Date db.Date `json:"date"` // required for "price" and "volume"
	Sort string  `json:"sort" choices:",ascending,descending"`
}

var _ message.Message = &Column{}

func (e *Column) InitMessage(js any) error {
	if err := message.Init(e, js); err != nil {
		return errors.Annotate(err, "failed to init Column")
	}
	switch e.Kind {
	case "price", "volume":
		if e.Date.IsZero() {
			return errors.Reason("date is required for kind=%s", e.Kind)
		}
	}
	return nil
}

func (e *Column) Header() string {
	switch e.Kind {
	case "ticker":
		return "Ticker"
	case "name":
		return "Name"
	case "exchange":
		return "Exchange"
	case "category":
		return "Category"
	case "sector":
		return "Sector"
	case "industry":
		return "Industry"
	case "price":
		return fmt.Sprintf("Split+Div Adjusted Close %s", e.Date)
	case "volume":
		return fmt.Sprintf("Cash Volume %s", e.Date)
	}
	return ""
}

type Config struct {
	Data    *db.Reader `json:"data" required:"true"`
	Columns []Column   `json:"columns"` // default: [{"kind": "ticker"}]
}

var _ message.Message = &Config{}

func (c *Config) InitMessage(js any) error {
	if err := message.Init(c, js); err != nil {
		return errors.Annotate(err, "failed to init Config")
	}
	if len(c.Columns) == 0 {
		c.Columns = []Column{{Kind: "ticker"}}
	}
	return nil
}

type Flags struct {
	LogLevel logging.Level
	Config   string // config of db.Reader
	CSV      bool   // dump CSV format; default: text.
}

func parseFlags(args []string) (*Flags, error) {
	var flags Flags
	fs := flag.NewFlagSet("parfait-list", flag.ExitOnError)
	flags.LogLevel = logging.Info
	fs.Var(&flags.LogLevel, "log-level", "Log level: debug, info, warning, error")
	fs.StringVar(&flags.Config, "conf", "", "config file (required)")
	fs.BoolVar(&flags.CSV, "csv", false, "print table in CSV format; default: text")

	err := fs.Parse(args)
	if err != nil {
		return nil, err
	}
	if flags.Config == "" {
		return nil, errors.Reason("missing required -conf argument")
	}
	return &flags, err
}

type Cell struct {
	IsNumber bool // which field to use as a value
	number   float64
	string   string
}

func (c Cell) String() string {
	if c.IsNumber {
		return fmt.Sprintf("%.2f", c.number)
	}
	return c.string
}

func (c Cell) Less(c2 Cell) bool {
	if c.IsNumber != c2.IsNumber {
		// All strings are smaller than numbers. Most often, this will happen with a
		// zero value which is an empty string, and zero (somewhat arbitrarily)
		// feels smaller.
		return !c.IsNumber
	}
	if c.IsNumber {
		return c.number < c2.number
	}
	return c.string < c2.string
}

func String(s string) Cell {
	return Cell{string: s}
}

func Number(n float64) Cell {
	return Cell{IsNumber: true, number: n}
}

type Row []Cell

var _ table.Row = Row{}

func (r Row) CSV() []string {
	res := make([]string, len(r))
	for i, c := range r {
		res[i] = c.String()
	}
	return res
}

func cachePrices(reader *db.Reader, ticker string, prices []db.PriceRow) ([]db.PriceRow, error) {
	if len(prices) > 0 {
		return prices, nil
	}
	prices, err := reader.Prices(ticker)
	if err != nil {
		return nil, errors.Annotate(err, "failed to read prices for %s", ticker)
	}
	return prices, nil
}

func processTicker(reader *db.Reader, cols []Column, ticker string) (Row, error) {
	tr, err := reader.TickerRow(ticker)
	if err != nil {
		return nil, errors.Annotate(err, "failed to read ticker row for %s", ticker)
	}
	var prices []db.PriceRow
	cells := make([]Cell, len(cols))
	for i, col := range cols {
		switch col.Kind {
		case "ticker":
			cells[i] = String(ticker)
		case "name":
			cells[i] = String(tr.Name)
		case "exchange":
			cells[i] = String(tr.Exchange)
		case "category":
			cells[i] = String(tr.Category)
		case "sector":
			cells[i] = String(tr.Sector)
		case "industry":
			cells[i] = String(tr.Industry)
		case "price":
			prices, err = cachePrices(reader, ticker, prices)
			if err != nil {
				return nil, errors.Annotate(err, "failed to read prices for %s", ticker)
			}
			for _, p := range prices {
				if p.Date == col.Date {
					cells[i] = Number(float64(p.CloseFullyAdjusted))
				}
			}
		case "volume":
			prices, err = cachePrices(reader, ticker, prices)
			if err != nil {
				return nil, errors.Annotate(err, "failed to read prices for %s", ticker)
			}
			for _, p := range prices {
				if p.Date == col.Date {
					cells[i] = Number(float64(p.CashVolume))
				}
			}
		}
	}
	return Row(cells), nil
}

func processData(ctx context.Context, reader *db.Reader, cols []Column) (*table.Table, error) {
	tickers, err := reader.Tickers(ctx)
	if err != nil {
		return nil, errors.Annotate(err, "failed to read tickers")
	}
	header := make([]string, len(cols))
	sortIdx := -1 // column index to sort by
	for i, col := range cols {
		header[i] = col.Header()
		if col.Sort != "" {
			sortIdx = i
		}
	}
	f := func(ticker string) Row {
		row, err := processTicker(reader, cols, ticker)
		if err != nil {
			logging.Warningf(ctx, "failed to process %s: %s", ticker, err.Error())
			return nil
		}
		return row
	}
	pm := iterator.ParallelMap(ctx, runtime.NumCPU(), iterator.FromSlice(tickers), f)
	defer pm.Close()

	rows := iterator.Reduce[Row, []Row](pm, []Row{}, func(r Row, rows []Row) []Row {
		return append(rows, r)
	})
	if sortIdx >= 0 {
		if cols[sortIdx].Sort == "ascending" {
			sort.Slice(rows, func(i, j int) bool {
				return rows[i][sortIdx].Less(rows[j][sortIdx])
			})
		} else {
			sort.Slice(rows, func(i, j int) bool {
				return rows[j][sortIdx].Less(rows[i][sortIdx])
			})
		}
	}
	tbl := table.NewTable(header...)
	for _, row := range rows {
		tbl.AddRow(row)
	}
	return tbl, nil
}

func printData(ctx context.Context, flags *Flags, w io.Writer) error {
	var config Config
	if err := message.FromFile(&config, flags.Config); err != nil {
		return errors.Annotate(err, "failed to read config '%s'", flags.Config)
	}
	tbl, err := processData(ctx, config.Data, config.Columns)
	if err != nil {
		return errors.Annotate(err, "failed to read data")
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
