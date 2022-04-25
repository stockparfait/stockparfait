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

package sharadar

import (
	"context"
	"io"
	"sort"

	"github.com/stockparfait/errors"
	"github.com/stockparfait/stockparfait/db"
	"github.com/stockparfait/stockparfait/ndl"
)

type TableName string

const (
	TickersTable  = TableName("TICKERS")
	ActionsTable  = TableName("ACTIONS")
	EquitiesTable = TableName("SEP")
	FundsTable    = TableName("SFP")
)

func FullTableName(table TableName) string {
	return "SHARADAR/" + string(table)
}

// FetchTickers returns a transparently paging iterator over RowTickers. When no
// tables are supplied, the default is all tables.
func FetchTickers(ctx context.Context, tables ...TableName) *ndl.RowIterator {
	q := ndl.NewTableQuery(FullTableName(TickersTable))
	if len(tables) > 0 {
		strs := make([]string, len(tables))
		for i, t := range tables {
			strs[i] = string(t)
		}
		q = q.Equal("table", strs...)
	}
	return q.Read(ctx)
}

// FetchActions returns a transparently paging iterator over Action. If no
// actions are specified, the default is all actions.
func FetchActions(ctx context.Context, actions ...ActionType) *ndl.RowIterator {
	q := ndl.NewTableQuery(FullTableName(ActionsTable))
	if len(actions) > 0 {
		strs := make([]string, len(actions))
		for i, a := range actions {
			strs[i] = a.String()
		}
		q = q.Equal("action", strs...)
	}
	return q.Read(ctx)
}

// Dataset for downloading and converting NDL Sharadar SEP/SFP data to
// db.Database.
type Dataset struct {
	Tickers    map[string]db.TickerRow
	RawActions map[string][]Action
	Actions    map[string][]db.ActionRow
	Prices     map[string][]db.PriceRow
}

// NewDataset initializes an empty Sharadar dataset.
func NewDataset() *Dataset {
	return &Dataset{
		Tickers:    make(map[string]db.TickerRow),
		RawActions: make(map[string][]Action),
		Actions:    make(map[string][]db.ActionRow),
		Prices:     make(map[string][]db.PriceRow),
	}
}

// FetchTickers for the given tables (default: all tables) and convert them to
// the standard database format.
func (d *Dataset) FetchTickers(ctx context.Context, tables ...TableName) error {
	it := FetchTickers(ctx, tables...)
	for {
		var t Ticker
		ok, err := it.Next(&t)
		if err != nil {
			return errors.Annotate(err, "failed to read tickers")
		}
		if !ok {
			break
		}
		d.Tickers[t.Ticker] = db.TickerRow{
			Exchange:    t.Exchange,
			Name:        t.Name,
			Category:    t.Category,
			Sector:      t.Sector,
			Industry:    t.Industry,
			Location:    t.Location,
			SECFilings:  t.SECFilings,
			CompanySite: t.CompanySite,
		}
	}
	return nil
}

// FetchActions downloads "raw" Sharadar actions filtered by 'actions'. If no
// actions are specified, the default is all actions.
func (d *Dataset) FetchActions(ctx context.Context, actions ...ActionType) error {
	it := FetchActions(ctx, actions...)
	for {
		var a Action
		ok, err := it.Next(&a)
		if !ok {
			if err != nil {
				return errors.Annotate(err, "failed to read actions")
			}
			break
		}
		d.RawActions[a.Ticker] = append(d.RawActions[a.Ticker], a)
	}
	for _, actions := range d.RawActions {
		sort.Slice(actions, func(i, j int) bool {
			return actions[i].Date.Before(actions[j].Date)
		})
	}
	return nil
}

// BulkDownloadPrices downloads daily prices using bulk download API.
func (d *Dataset) BulkDownloadPrices(ctx context.Context, table TableName) error {
	fullTable := FullTableName(table)
	h, err := ndl.BulkDownload(ctx, fullTable)
	if err != nil {
		return errors.Annotate(err, "failed to initiate bulk download of %s", table)
	}
	if h.Status != ndl.StatusFresh && h.Status != ndl.StatusRegenerating {
		return errors.Reason(
			"table %s is not ready for bulk download, status=%s", table, h.Status)
	}
	var interval int64 = 10 * 1024 * 1024 // log every 10MB
	h.MonitorFactory = ndl.LoggingMonitorFactory(ctx, fullTable, interval)
	r, err := ndl.BulkDownloadCSV(ctx, h)
	if err != nil {
		return errors.Annotate(err, "failed to bulk-download CSV data of %s", table)
	}
	defer r.Close()

	header, err := r.Read()
	if err != nil {
		return errors.Annotate(err, "failed to read CSV header")
	}
	colMap, err := PriceSchema.MapCSVColumns(header)
	if err != nil {
		return errors.Annotate(err, "unexpected CSV header")
	}

	for {
		row, err := r.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return errors.Annotate(err, "failed to read CSV")
		}
		var p Price
		if err := p.FromCSV(row, colMap); err != nil {
			return errors.Annotate(err, "failed to parse CSV row")
		}
		d.Prices[p.Ticker] = append(d.Prices[p.Ticker], db.PriceRow{
			Date:               p.Date,
			Close:              p.CloseUnadjusted,
			CloseSplitAdjusted: p.Close,
			CloseFullyAdjusted: p.CloseAdjusted,
			DollarVolume:       p.Close * p.Volume,
		})
	}

	for _, prices := range d.Prices {
		sort.Slice(prices, func(i, j int) bool {
			return prices[i].Date.Before(prices[j].Date)
		})
	}
	return nil
}
