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

package db

import (
	"context"
	"encoding/gob"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"

	"github.com/stockparfait/errors"
)

type contextKey int

const (
	dbContextKey contextKey = iota
)

// UseDB injects database into the context.
func UseDB(ctx context.Context, db *Database) context.Context {
	return context.WithValue(ctx, dbContextKey, db)
}

// GetDB extracts database from the context.
func GetDB(ctx context.Context) *Database {
	db, ok := ctx.Value(dbContextKey).(*Database)
	if !ok {
		return nil
	}
	return db
}

func writeGob(fileName string, v interface{}) error {
	f, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return errors.Annotate(err, "failed to open file for writing: '%s'", fileName)
	}
	defer f.Close()
	enc := gob.NewEncoder(f)
	if err = enc.Encode(v); err != nil {
		return errors.Annotate(err, "failed to write to '%s'", fileName)
	}
	return nil
}

func readGob(fileName string, v interface{}) error {
	f, err := os.Open(fileName)
	if err != nil {
		return errors.Annotate(err, "failed to open file for reading: '%s'", fileName)
	}
	defer f.Close()
	dec := gob.NewDecoder(f)
	if err = dec.Decode(v); err != nil {
		return errors.Annotate(err, "failed to read from '%s'", fileName)
	}
	return nil
}

type Database struct {
	cachePath     string
	tickers       map[string]TickerRow
	actions       map[string][]ActionRow
	monthly       map[string][]ResampledRow
	metadata      Metadata
	tickersOnce   sync.Once
	tickersError  error
	actionsOnce   sync.Once
	actionsError  error
	monthlyOnce   sync.Once
	monthlyError  error
	mkdirOnce     sync.Once
	mkdirError    error
	metadataOnce  sync.Once
	metadataError error
}

func NewDatabase(cachePath string) *Database {
	return &Database{
		cachePath: cachePath,
		tickers:   make(map[string]TickerRow),
		actions:   make(map[string][]ActionRow),
		monthly:   make(map[string][]ResampledRow),
	}
}

func (db *Database) tickersFile() string {
	return filepath.Join(db.cachePath, "tickers.gob")
}

func (db *Database) actionsFile() string {
	return filepath.Join(db.cachePath, "actions.gob")
}

func (db *Database) pricesDir() string {
	return filepath.Join(db.cachePath, "prices")
}

func (db *Database) pricesFile(ticker string) string {
	return filepath.Join(db.pricesDir(), ticker+".gob")
}

func (db *Database) monthlyFile() string {
	return filepath.Join(db.cachePath, "monthly.gob")
}

func (db *Database) metadataFile() string {
	return filepath.Join(db.cachePath, "metadata.json")
}

func (db *Database) cacheMetadata() error {
	db.metadataOnce.Do(func() {
		fileName := db.metadataFile()
		f, err := os.Open(fileName)
		if err != nil {
			db.metadataError = errors.Annotate(err,
				"failed to open file for reading: '%s'", fileName)
		}
		defer f.Close()

		dec := json.NewDecoder(f)
		if err := dec.Decode(&db.metadata); err != nil {
			db.metadataError = errors.Annotate(err, "failed to decode JSON")
		}
	})
	return db.metadataError
}

func (db *Database) cacheTickers() error {
	db.tickersOnce.Do(func() {
		if err := readGob(db.tickersFile(), &db.tickers); err != nil {
			db.tickersError = errors.Annotate(
				err, "failed to load %s", db.tickersFile())
		}
	})
	return db.tickersError
}

func (db *Database) cacheActions() error {
	db.actionsOnce.Do(func() {
		if err := readGob(db.actionsFile(), &db.actions); err != nil {
			db.actionsError = errors.Annotate(
				err, "failed to load %s", db.actionsFile())
		}
	})
	return db.actionsError
}

func (db *Database) cacheMonthly() error {
	db.monthlyOnce.Do(func() {
		if err := readGob(db.monthlyFile(), &db.monthly); err != nil {
			db.monthlyError = errors.Annotate(
				err, "failed to load %s", db.monthlyFile())
		}
	})
	return db.monthlyError
}

func (db *Database) createDirs() error {
	db.mkdirOnce.Do(func() {
		if err := os.MkdirAll(db.pricesDir(), os.ModeDir|0755); err != nil {
			db.mkdirError = errors.Annotate(
				err, "failed to create %s", db.pricesDir())
		}
	})
	return db.mkdirError
}

// WriteTickers saves the tickers table to the DB file, and sets the number of
// tickers in the metadata.
func (db *Database) WriteTickers(tickers map[string]TickerRow) error {
	if err := db.createDirs(); err != nil {
		return errors.Annotate(err, "failed to create DB directories")
	}
	if err := writeGob(db.tickersFile(), tickers); err != nil {
		return errors.Annotate(err, "failed to write '%s'", db.tickersFile())
	}
	db.metadata.NumTickers = len(tickers)
	return nil
}

// WriteActions saves the actions table to the DB file, and sets the number of
// actions in the metadata. Actions are indexed by ticker, and for each ticker
// actions are assumed to be sorted by date.
func (db *Database) WriteActions(actions map[string][]ActionRow) error {
	if err := db.createDirs(); err != nil {
		return errors.Annotate(err, "failed to create DB directories")
	}
	if err := writeGob(db.actionsFile(), actions); err != nil {
		return errors.Annotate(err, "failed to write '%s'", db.actionsFile())
	}
	db.metadata.NumActions = 0
	for _, as := range actions {
		db.metadata.NumActions += len(as)
	}
	return nil
}

// WritePrices saves the ticker prices to the DB file and incrementally updates
// the metadata.  Prices are assumed to be sorted by date.
func (db *Database) WritePrices(ticker string, prices []PriceRow) error {
	if err := db.createDirs(); err != nil {
		return errors.Annotate(err, "failed to create DB directories")
	}
	if err := writeGob(db.pricesFile(ticker), prices); err != nil {
		return errors.Annotate(err, "failed to write '%s'", db.pricesFile(ticker))
	}
	db.metadata.NumPrices += len(prices)
	for _, p := range prices {
		if db.metadata.Start.IsZero() || db.metadata.Start.After(p.Date) {
			db.metadata.Start = p.Date
		}
		if db.metadata.End.IsZero() || db.metadata.End.Before(p.Date) {
			db.metadata.End = p.Date
		}
	}
	return nil
}

// ComputeMonthly converts daily price series into resampled monthly price
// series.
func ComputeMonthly(prices []PriceRow) []ResampledRow {
	if len(prices) == 0 {
		return nil
	}
	abs32 := func(x float32) float32 {
		if x < 0.0 {
			return -x
		}
		return x
	}

	res := []ResampledRow{}
	var currMonth Date
	var currRes ResampledRow
	var prevClose float32
	for _, p := range prices {
		if currMonth != p.Date.MonthStart() {
			if !currMonth.IsZero() {
				res = append(res, currRes)
			}
			prevClose = 0.0 // do not add cross-month volatility
			currRes = ResampledRow{
				OpenSplitAdjusted: p.CloseSplitAdjusted,
				DateOpen:          p.Date,
			}
		}
		currMonth = p.Date.MonthStart()
		relMove := abs32(p.CloseSplitAdjusted - prevClose)
		if prevClose > 0.0 {
			relMove = relMove / prevClose
		} else {
			relMove = 0.0
		}
		prevClose = p.CloseSplitAdjusted
		currRes.Close = p.CloseUnadjusted()
		currRes.CloseSplitAdjusted = p.CloseSplitAdjusted
		currRes.CloseFullyAdjusted = p.CloseFullyAdjusted
		currRes.CashVolume += p.CashVolume
		currRes.DateClose = p.Date
		currRes.SumRelativeMove += relMove
		currRes.NumSamples++
		currRes.Active = p.Active()
	}
	res = append(res, currRes)
	return res
}

// WriteMonthly saves the monthly resampled table to the DB file and sets the
// number of samples in the metadata. ResampledRow's are indexed by ticker, and
// for each ticker are assumed to be sorted by the closing date.
func (db *Database) WriteMonthly(monthly map[string][]ResampledRow) error {
	if err := db.createDirs(); err != nil {
		return errors.Annotate(err, "failed to create DB directories")
	}
	if err := writeGob(db.monthlyFile(), monthly); err != nil {
		return errors.Annotate(err, "failed to write '%s'", db.monthlyFile())
	}
	db.metadata.NumMonthly = 0
	for _, ms := range monthly {
		db.metadata.NumMonthly += len(ms)
	}
	return nil
}

// WriteMetadata saves the metadata accumulated by the Write* methods. It is
// stored in JSON format to be human-readable.
func (db *Database) WriteMetadata() error {
	fileName := db.metadataFile()
	f, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return errors.Annotate(err, "failed to open file for writing: '%s'", fileName)
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(db.metadata); err != nil {
		return errors.Annotate(err, "failed to write to '%s'", fileName)
	}
	return nil
}

// Metadata for the database. It is cached in memory upon the first call.
func (db *Database) Metadata() (Metadata, error) {
	if err := db.cacheMetadata(); err != nil {
		return Metadata{}, errors.Annotate(err, "failed to load metadata")
	}
	return db.metadata, nil
}

// TickerRow for the given ticker. It's an error if a ticker is not in DB.
// Tickers are cached in memory upon the first call. Go routine safe.
func (db *Database) TickerRow(ticker string) (TickerRow, error) {
	if err := db.cacheTickers(); err != nil {
		return TickerRow{}, errors.Annotate(err, "failed to load tickers")
	}
	r, ok := db.tickers[ticker]
	if !ok {
		return TickerRow{}, errors.Reason("no such ticker: %s", ticker)
	}
	return r, nil
}

// Tickers returns the list of tickers satisfying TickerRow data constraints.
// Tickers are cached in memory upon the first call. Go routine safe.
func (db *Database) Tickers(c *Constraints) ([]string, error) {
	if err := db.cacheTickers(); err != nil {
		return nil, errors.Annotate(err, "failed to load tickers")
	}
	tickers := []string{}
	for t, r := range db.tickers {
		if c.CheckTicker(t) && c.CheckTickerRow(r) {
			tickers = append(tickers, t)
		}
	}
	return tickers, nil
}

// Actions for ticker satisfying the constraints, sorted by date. Actions for
// all tickers are cached in memory upon the first call. Go routine safe.
func (db *Database) Actions(ticker string, c *Constraints) ([]ActionRow, error) {
	if err := db.cacheActions(); err != nil {
		return nil, errors.Annotate(err, "failed to load actions")
	}
	actions, ok := db.actions[ticker]
	if !ok {
		return nil, errors.Reason("no actions found for ticker %s", ticker)
	}
	res := []ActionRow{}
	for _, a := range actions {
		if c.CheckAction(a) {
			res = append(res, a)
		}
	}
	return res, nil
}

// Prices for ticker satilfying constraints, sorted by date. Prices are NOT
// cached in memory, and are read from disk every time. It is probably go
// routine safe, if the underlying OS allows to open and read the same file
// multiple times from the same process. Reading different tickers is definitely
// safe in parallel.
func (db *Database) Prices(ticker string, c *Constraints) ([]PriceRow, error) {
	prices := []PriceRow{}
	if err := readGob(db.pricesFile(ticker), &prices); err != nil {
		return nil, errors.Annotate(err, "failed to read prices for %s", ticker)
	}
	res := []PriceRow{}
	for _, p := range prices {
		if c.CheckPrice(p) {
			res = append(res, p)
		}
	}
	return res, nil
}

// Monthly price data for ticker satisfying the constraints, sorted by date.
// Data for all tickers are cached in memory upon the first call. Go routine
// safe.
func (db *Database) Monthly(ticker string, c *Constraints) ([]ResampledRow, error) {
	if err := db.cacheMonthly(); err != nil {
		return nil, errors.Annotate(err, "failed to load monthly data")
	}
	monthly, ok := db.monthly[ticker]
	if !ok {
		return nil, errors.Reason("no monthly data found for ticker %s", ticker)
	}
	res := []ResampledRow{}
	for _, r := range monthly {
		if c.CheckResampled(r) {
			res = append(res, r)
		}
	}
	return res, nil
}
