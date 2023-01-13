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
	"runtime"
	"sort"
	"strings"

	"github.com/stockparfait/errors"
	"github.com/stockparfait/iterator"
	"github.com/stockparfait/logging"
	"github.com/stockparfait/stockparfait/db"
	"github.com/stockparfait/stockparfait/ndl"
)

type TableName = string

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
	Tickers       map[string]db.TickerRow
	RawActions    map[string][]Action
	Prices        map[string][]db.PriceRow
	Monthly       map[string][]db.ResampledRow
	NumRawActions int
	NumPrices     int
}

// NewDataset initializes an empty Sharadar dataset.
func NewDataset() *Dataset {
	return &Dataset{
		Tickers:    make(map[string]db.TickerRow),
		RawActions: make(map[string][]Action),
		Prices:     make(map[string][]db.PriceRow),
		Monthly:    make(map[string][]db.ResampledRow),
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
			Source:      t.TableName,
			Exchange:    t.Exchange,
			Name:        t.Name,
			Category:    t.Category,
			Sector:      t.Sector,
			Industry:    t.Industry,
			Location:    t.Location,
			SECFilings:  t.SECFilings,
			CompanySite: t.CompanySite,
			Active:      !t.IsDelisted,
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
		d.NumRawActions++
	}
	for _, actions := range d.RawActions {
		sort.Slice(actions, func(i, j int) bool {
			return actions[i].Date.Before(actions[j].Date)
		})
	}
	return nil
}

type pricesResult struct {
	Prices map[string][]db.PriceRow
	Error  error
}

type pricesJobsIter struct {
	Reader    *ndl.CSVReader
	Error     error // from the CSVReader
	ColMap    map[string]int
	BatchSize int // for efficient parallelization should be at least 1000
	Done      bool
}

var _ iterator.Iterator[func() pricesResult] = &pricesJobsIter{}

func (it *pricesJobsIter) Next() (func() pricesResult, bool) {
	if it.BatchSize <= 0 {
		it.Error = errors.Reason("batch size = %d must be > 0", it.BatchSize)
		return nil, false
	}
	if it.Done {
		return nil, false
	}
	rows := [][]string{}
	for i := 0; i < it.BatchSize; i++ {
		row, err := it.Reader.Read()
		if err != nil {
			if err != io.EOF {
				it.Error = err
			}
			it.Done = true
			break
		}
		rows = append(rows, row)
	}
	if len(rows) == 0 {
		it.Done = true
		return nil, false
	}
	job := func() pricesResult {
		prices := map[string][]db.PriceRow{}
		for _, row := range rows {
			var p Price
			if err := p.FromCSV(row, it.ColMap); err != nil {
				return pricesResult{
					Error: errors.Annotate(err, "failed to parse CSV row"),
				}
			}
			prices[p.Ticker] = append(prices[p.Ticker], db.PriceRow{
				Date:               p.Date,
				Close:              p.CloseUnadjusted,
				CloseSplitAdjusted: p.Close,
				CloseFullyAdjusted: p.CloseAdjusted,
				CashVolume:         p.Close * p.Volume,
			})
		}
		return pricesResult{
			Prices: prices,
		}
	}
	return job, true
}

// BulkDownloadPrices downloads daily prices using bulk download API. It must be
// run after downloading TICKERS table, since it will skip any ticker not in
// TICKERS.
func (d *Dataset) BulkDownloadPrices(ctx context.Context, table TableName) error {
	fullTable := FullTableName(table)
	logging.Infof(ctx, "initiating bulk download of %s prices", table)
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

	logging.Infof(ctx, "unzipping the prices CSV file...")
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	jobs := &pricesJobsIter{
		Reader:    r,
		ColMap:    colMap,
		BatchSize: 10000,
	}
	f := func(f func() pricesResult) pricesResult { return f() }
	m := iterator.ParallelMap[func() pricesResult, pricesResult](cctx, runtime.NumCPU(), jobs, f)
	for pr, ok := m.Next(); ok; pr, ok = m.Next() {
		if pr.Error != nil {
			cancel()
			for ; ok; _, ok = m.Next() { // flush the iterator
			}
			return errors.Annotate(pr.Error, "failed to parse CSV")
		}
		for t, prices := range pr.Prices {
			if _, ok := d.Tickers[t]; !ok {
				logging.Warningf(ctx, "skipping %s prices, it's not in TICKERS table", t)
				continue
			}
			for _, p := range prices {
				d.Prices[t] = append(d.Prices[t], p)
				d.NumPrices++
				if d.NumPrices%1000_000 == 0 {
					logging.Debugf(ctx, "unzipped %dM prices", d.NumPrices/1000_000)
				}
			}
		}
	}
	if jobs.Error != nil {
		return errors.Annotate(jobs.Error, "failed to read CSV")
	}

	logging.Infof(ctx, "sorting prices...")
	numTickersSorted := 0
	for _, prices := range d.Prices {
		sort.Slice(prices, func(i, j int) bool {
			return prices[i].Date.Before(prices[j].Date)
		})
		numTickersSorted++
		if numTickersSorted%1000 == 0 {
			logging.Debugf(ctx, "sorted prices for %d tickers out of %d",
				numTickersSorted, len(d.Prices))
		}
	}
	logging.Infof(ctx, "done sorting")
	return nil
}

// DownloadAll - tickers, actions and prices for the requested tables.
func (d *Dataset) DownloadAll(ctx context.Context, dbPath, dbName string, tables ...TableName) error {
	if len(tables) == 0 {
		tables = []TableName{EquitiesTable, FundsTable}
	}
	logging.Infof(ctx, "fetching tickers for %s...", strings.Join(tables, ", "))
	if err := d.FetchTickers(ctx, tables...); err != nil {
		return errors.Annotate(err, "failed to fetch tickers")
	}
	logging.Infof(ctx, "downloaded %d tickers", len(d.Tickers))
	currPrices := 0
	for _, t := range tables {
		logging.Infof(ctx, "bulk-downloading %s prices", t)
		if err := d.BulkDownloadPrices(ctx, t); err != nil {
			return errors.Annotate(err, "failed to download %s price table", t)
		}
		logging.Infof(ctx, "downloaded %d %s prices", d.NumPrices-currPrices, t)
		currPrices = d.NumPrices
	}
	logging.Infof(ctx, "downloaded total %d prices", d.NumPrices)
	logging.Infof(ctx, "writing tickers...")
	w := db.NewWriter(dbPath, dbName)
	if err := w.WriteTickers(d.Tickers); err != nil {
		return errors.Annotate(err, "failed to write tickers")
	}
	logging.Infof(ctx, "writing prices...")
	for ticker, prices := range d.Prices {
		if err := w.WritePrices(ticker, prices); err != nil {
			return errors.Annotate(err, "failed to write prices for %s", ticker)
		}
		d.Monthly[ticker] = db.ComputeMonthly(prices)
	}
	logging.Infof(ctx, "writing monthly resampled prices...")
	if err := w.WriteMonthly(d.Monthly); err != nil {
		return errors.Annotate(err, "failed to write monthly prices")
	}
	logging.Infof(ctx, "writing metadata...")
	if err := w.WriteMetadata(w.Metadata); err != nil {
		return errors.Annotate(err, "failed to write metadata")
	}
	logging.Infof(ctx, "cleaning up...")
	if err := db.Cleanup(ctx, dbPath, dbName); err != nil {
		return errors.Annotate(err, "failed to clean up DB")
	}
	logging.Infof(ctx, "all done.")
	return nil
}
