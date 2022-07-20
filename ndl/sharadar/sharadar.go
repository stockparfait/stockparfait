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
	"github.com/stockparfait/logging"
	"github.com/stockparfait/parallel"
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
	Actions       map[string][]db.ActionRow
	Prices        map[string][]db.PriceRow
	Monthly       map[string][]db.ResampledRow
	NumRawActions int
	NumActions    int
	NumPrices     int
}

// NewDataset initializes an empty Sharadar dataset.
func NewDataset() *Dataset {
	return &Dataset{
		Tickers:    make(map[string]db.TickerRow),
		RawActions: make(map[string][]Action),
		Actions:    make(map[string][]db.ActionRow),
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
	ColMap    map[string]int
	BatchSize int // for efficient parallelization should be at least 1000
	Done      bool
}

var _ parallel.JobsIter = &pricesJobsIter{}

func (it *pricesJobsIter) Next() (parallel.Job, error) {
	if it.BatchSize <= 0 {
		return nil, errors.Reason("batch size = %d must be > 0", it.BatchSize)
	}
	if it.Done {
		return nil, parallel.Done
	}
	rows := [][]string{}
	for i := 0; i < it.BatchSize; i++ {
		row, err := it.Reader.Read()
		if err == io.EOF {
			it.Done = true
			break
		}
		rows = append(rows, row)
	}
	if len(rows) == 0 {
		it.Done = true
		return nil, parallel.Done
	}
	job := func() interface{} {
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
	return job, nil
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

	m := parallel.Map(cctx, runtime.NumCPU(), &pricesJobsIter{
		Reader:    r,
		ColMap:    colMap,
		BatchSize: 10000,
	})
	for {
		v, err := m.Next()
		if err != nil {
			if err == parallel.Done {
				break
			}
			return errors.Annotate(err, "failed to read CSV")
		}
		pr, ok := v.(pricesResult)
		if !ok {
			return errors.Reason("incorrect result type: %T", v)
		}
		if pr.Error != nil {
			cancel() // flush the parallel jobs
			for err == nil {
				_, err = m.Next()
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

// ComputeActions from RawActions and Prices of the Dataset, and update the
// Active bit in Prices. This method must be called after all the data is
// downloaded, but before saving the prices. It combines multiple actions
// between price points, creating at most one action per price point.
func (d *Dataset) ComputeActions(ctx context.Context) {
	// For each ticker, step through the prices and actions in sync by date, and
	// generate the appropriate DB actions.
	for ticker, prices := range d.Prices {
		if len(prices) == 0 {
			continue
		}
		rawActions := d.RawActions[ticker]

		actions := []db.ActionRow{}
		var prevPrice float32 // split-adjusted previous close
		ai := 0               // action index
		active := true
		for i, price := range prices {
			action := db.ActionRow{
				Date:           price.Date,
				DividendFactor: 1.0,
				SplitFactor:    1.0,
				Active:         true,
			}
			hasActions := false
			for ; ai < len(rawActions) && !rawActions[ai].Date.After(price.Date); ai++ {
				switch ra := rawActions[ai]; ra.Action {
				case ListedAction:
					action.Active = true
					hasActions = true
				case AcquisitionByAction, MergerFromAction, RegulatoryDelistingAction, VoluntaryDelistingAction, DelistedAction:
					action.Active = false
					hasActions = true
				case DividendAction, SpinoffDividendAction:
					if prevPrice > 0.0 { // no previous price at the first sample
						action.DividendFactor *= (prevPrice - ra.Value) / prevPrice
						hasActions = true
					}
				case SplitAction:
					if ra.Value > 0.0 { // split factor cannot meaningfully be <= 0.0
						action.SplitFactor *= 1.0 / ra.Value
						hasActions = true
					}
				}
			}
			prevPrice = price.CloseSplitAdjusted
			// Special case: no action at the start. Inject "listed" action.
			if i == 0 && !hasActions {
				action = db.ActionRow{
					Date:           price.Date,
					Active:         true,
					DividendFactor: 1.0,
					SplitFactor:    1.0,
				}
				hasActions = true
			}
			if hasActions {
				actions = append(actions, action)
				active = action.Active
			}
			prices[i].SetActive(active)
		}
		// Make sure that the actions' active status agrees with the ticker's.
		t := d.Tickers[ticker]
		if actions[len(actions)-1].Active != t.Active {
			lastPrice := prices[len(prices)-1]
			if actions[len(actions)-1].Date == lastPrice.Date {
				actions[len(actions)-1].Active = t.Active
			} else {
				actions = append(actions, db.ActionRow{
					Date:           lastPrice.Date,
					DividendFactor: 1.0,
					SplitFactor:    1.0,
					Active:         t.Active,
				})
			}
			prices[len(prices)-1].SetActive(t.Active)
		}
		d.Actions[ticker] = actions
		d.NumActions += len(actions)
	}
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
	logging.Infof(ctx, "fetching actions...")
	if err := d.FetchActions(ctx, RelevantActions...); err != nil {
		return errors.Annotate(err, "failed to fetch actions")
	}
	logging.Infof(ctx, "downloaded %d actions", d.NumRawActions)
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
	logging.Infof(ctx, "processing actions...")
	d.ComputeActions(ctx)
	logging.Infof(ctx, "created %d DB actions", d.NumActions)
	logging.Infof(ctx, "writing tickers...")
	w := db.NewWriter(dbPath, dbName)
	if err := w.WriteTickers(d.Tickers); err != nil {
		return errors.Annotate(err, "failed to write tickers")
	}
	logging.Infof(ctx, "writing actions...")
	if err := w.WriteActions(d.Actions); err != nil {
		return errors.Annotate(err, "failed to write actions")
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
	if err := w.WriteMetadata(); err != nil {
		return errors.Annotate(err, "failed to write metadata")
	}
	logging.Infof(ctx, "all done.")
	return nil
}
