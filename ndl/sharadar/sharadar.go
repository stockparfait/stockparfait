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

	"github.com/stockparfait/errors"
	"github.com/stockparfait/stockparfait/db"
	"github.com/stockparfait/stockparfait/ndl"
)

type TableName string

const (
	EquitiesTable = TableName("SEP")
	FundsTable    = TableName("SFP")
)

// FetchTickers returns a transparently paging iterator over RowTickers. When no
// tables are supplied, the default are all tables.
func FetchTickers(ctx context.Context, tables ...TableName) *ndl.RowIterator {
	q := ndl.NewTableQuery("SHARADAR/TICKERS")
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
// actions are specified, the default are all actions.
func FetchActions(ctx context.Context, actions ...ActionType) *ndl.RowIterator {
	q := ndl.NewTableQuery("SHARADAR/ACTIONS")
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
//  the standard database format.
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
// actions are specified, the default are all actions.
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
	return nil
}
