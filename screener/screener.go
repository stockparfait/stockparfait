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

package screener

import (
	"context"
	"fmt"
	"runtime"
	"sort"

	"github.com/stockparfait/errors"
	"github.com/stockparfait/iterator"
	"github.com/stockparfait/logging"
	"github.com/stockparfait/stockparfait/db"
	"github.com/stockparfait/stockparfait/table"
)

// Cell of a table Row which is a union of string or number (float64).
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

func Screen(ctx context.Context, c *Config) (*table.Table, error) {
	tickers, err := c.Data.Tickers(ctx)
	if err != nil {
		return nil, errors.Annotate(err, "failed to read tickers")
	}
	header := make([]string, len(c.Columns))
	sortIdx := -1 // column index to sort by
	for i, col := range c.Columns {
		header[i] = col.Header()
		if col.Sort != "" {
			sortIdx = i
		}
	}
	f := func(ticker string) Row {
		row, err := processTicker(c.Data, c.Columns, ticker)
		if err != nil {
			logging.Warningf(ctx, "failed to process %s: %s", ticker, err.Error())
			return nil
		}
		return row
	}
	pm := iterator.ParallelMap(ctx, 2*runtime.NumCPU(), iterator.FromSlice(tickers), f)
	defer pm.Close()

	rows := iterator.Reduce[Row, []Row](pm, []Row{}, func(r Row, rows []Row) []Row {
		return append(rows, r)
	})
	if sortIdx >= 0 {
		less := func(i, j int) bool { return rows[i][sortIdx].Less(rows[j][sortIdx]) }
		if c.Columns[sortIdx].Sort == "descending" {
			less = func(i, j int) bool { return rows[j][sortIdx].Less(rows[i][sortIdx]) }
		}
		sort.Slice(rows, less)
	}
	tbl := table.NewTable(header...)
	for _, row := range rows {
		tbl.AddRow(row)
	}
	return tbl, nil
}
