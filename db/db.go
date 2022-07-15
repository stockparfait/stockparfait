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
	"encoding/gob"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"

	"github.com/stockparfait/errors"
	"github.com/stockparfait/stockparfait/message"
)

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

type Reader struct {
	Constraints   *Constraints
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
	metadataOnce  sync.Once
	metadataError error
}

func NewReader(cachePath string) *Reader {
	return &Reader{
		Constraints: NewConstraints(),
		cachePath:   cachePath,
		tickers:     make(map[string]TickerRow),
		actions:     make(map[string][]ActionRow),
		monthly:     make(map[string][]ResampledRow),
	}
}

func tickersFile(cachePath string) string {
	return filepath.Join(cachePath, "tickers.gob")
}

func actionsFile(cachePath string) string {
	return filepath.Join(cachePath, "actions.gob")
}

func pricesDir(cachePath string) string {
	return filepath.Join(cachePath, "prices")
}

func pricesFile(cachePath, ticker string) string {
	return filepath.Join(pricesDir(cachePath), ticker+".gob")
}

func monthlyFile(cachePath string) string {
	return filepath.Join(cachePath, "monthly.gob")
}

func metadataFile(cachePath string) string {
	return filepath.Join(cachePath, "metadata.json")
}

func (r *Reader) cacheMetadata() error {
	r.metadataOnce.Do(func() {
		fileName := metadataFile(r.cachePath)
		f, err := os.Open(fileName)
		if err != nil {
			r.metadataError = errors.Annotate(err,
				"failed to open file for reading: '%s'", fileName)
		}
		defer f.Close()

		dec := json.NewDecoder(f)
		if err := dec.Decode(&r.metadata); err != nil {
			r.metadataError = errors.Annotate(err, "failed to decode JSON")
		}
	})
	return r.metadataError
}

func (r *Reader) cacheTickers() error {
	r.tickersOnce.Do(func() {
		if err := readGob(tickersFile(r.cachePath), &r.tickers); err != nil {
			r.tickersError = errors.Annotate(
				err, "failed to load %s", tickersFile(r.cachePath))
		}
	})
	return r.tickersError
}

func (r *Reader) cacheActions() error {
	r.actionsOnce.Do(func() {
		if err := readGob(actionsFile(r.cachePath), &r.actions); err != nil {
			r.actionsError = errors.Annotate(
				err, "failed to load %s", actionsFile(r.cachePath))
		}
	})
	return r.actionsError
}

func (r *Reader) cacheMonthly() error {
	r.monthlyOnce.Do(func() {
		if err := readGob(monthlyFile(r.cachePath), &r.monthly); err != nil {
			r.monthlyError = errors.Annotate(
				err, "failed to load %s", monthlyFile(r.cachePath))
		}
	})
	return r.monthlyError
}

// Metadata for the database. It is cached in memory upon the first call.
func (r *Reader) Metadata() (Metadata, error) {
	if err := r.cacheMetadata(); err != nil {
		return Metadata{}, errors.Annotate(err, "failed to load metadata")
	}
	return r.metadata, nil
}

// TickerRow for the given ticker. It's an error if a ticker is not in R.
// Tickers are cached in memory upon the first call. Go routine safe.
func (r *Reader) TickerRow(ticker string) (TickerRow, error) {
	if err := r.cacheTickers(); err != nil {
		return TickerRow{}, errors.Annotate(err, "failed to load tickers")
	}
	row, ok := r.tickers[ticker]
	if !ok {
		return TickerRow{}, errors.Reason("no such ticker: %s", ticker)
	}
	return row, nil
}

// Tickers returns the list of tickers satisfying current Reader's constraints.
// All tickers are cached in memory, and tickers are filtered for each call.
// Therefore, modifying Reader's constraints takes effect at the next call
// without re-reading the tickers.  Go-routine safe assuming constraints are not
// modified.
func (r *Reader) Tickers() ([]string, error) {
	if err := r.cacheTickers(); err != nil {
		return nil, errors.Annotate(err, "failed to load tickers")
	}
	tickers := []string{}
	for t, row := range r.tickers {
		if r.Constraints.CheckTicker(t) && r.Constraints.CheckTickerRow(row) {
			tickers = append(tickers, t)
		}
	}
	return tickers, nil
}

// Actions for ticker satisfying the Reader's constraints, sorted by date.
// All actions for all tickers are cached in memory upon the first call. Go routine
// safe, assuming Reader's constraints are not modified.
func (r *Reader) Actions(ticker string) ([]ActionRow, error) {
	if err := r.cacheActions(); err != nil {
		return nil, errors.Annotate(err, "failed to load actions")
	}
	actions, ok := r.actions[ticker]
	if !ok {
		return nil, errors.Reason("no actions found for ticker %s", ticker)
	}
	res := []ActionRow{}
	for _, a := range actions {
		if r.Constraints.CheckAction(a) {
			res = append(res, a)
		}
	}
	return res, nil
}

// Prices for ticker satilfying Reader's constraints, sorted by date. Prices are
// NOT cached in memory, and are read from disk every time. It is probably go
// routine safe, if the underlying OS allows to open and read the same file
// multiple times from the same process. Reading different tickers is definitely
// safe in parallel, assuming consraints are not modified.
func (r *Reader) Prices(ticker string) ([]PriceRow, error) {
	prices := []PriceRow{}
	if err := readGob(pricesFile(r.cachePath, ticker), &prices); err != nil {
		return nil, errors.Annotate(err, "failed to read prices for %s", ticker)
	}
	res := []PriceRow{}
	for _, p := range prices {
		if r.Constraints.CheckPrice(p) {
			res = append(res, p)
		}
	}
	return res, nil
}

// Monthly price data for ticker satisfying Reader's constraints, sorted by
// date.  Data for all tickers are cached in memory upon the first call. Go
// routine safe assuming constraints are not modified.
func (r *Reader) Monthly(ticker string) ([]ResampledRow, error) {
	if err := r.cacheMonthly(); err != nil {
		return nil, errors.Annotate(err, "failed to load monthly data")
	}
	monthly, ok := r.monthly[ticker]
	if !ok {
		return nil, errors.Reason("no monthly data found for ticker %s", ticker)
	}
	res := []ResampledRow{}
	for _, row := range monthly {
		if r.Constraints.CheckResampled(row) {
			res = append(res, row)
		}
	}
	return res, nil
}

type Writer struct {
	cachePath  string
	metadata   Metadata
	mkdirOnce  sync.Once
	mkdirError error
}

func NewWriter(cachePath string) *Writer {
	return &Writer{cachePath: cachePath}
}

func (w *Writer) createDirs() error {
	w.mkdirOnce.Do(func() {
		if err := os.MkdirAll(pricesDir(w.cachePath), os.ModeDir|0755); err != nil {
			w.mkdirError = errors.Annotate(
				err, "failed to create %s", pricesDir(w.cachePath))
		}
	})
	return w.mkdirError
}

// WriteTickers saves the tickers table to the DB file, and sets the number of
// tickers in the metadata.
func (w *Writer) WriteTickers(tickers map[string]TickerRow) error {
	if err := w.createDirs(); err != nil {
		return errors.Annotate(err, "failed to create DB directories")
	}
	if err := writeGob(tickersFile(w.cachePath), tickers); err != nil {
		return errors.Annotate(err, "failed to write '%s'", tickersFile(w.cachePath))
	}
	w.metadata.NumTickers = len(tickers)
	return nil
}

// WriteActions saves the actions table to the DB file, and sets the number of
// actions in the metadata. Actions are indexed by ticker, and for each ticker
// actions are assumed to be sorted by date.
func (w *Writer) WriteActions(actions map[string][]ActionRow) error {
	if err := w.createDirs(); err != nil {
		return errors.Annotate(err, "failed to create DB directories")
	}
	if err := writeGob(actionsFile(w.cachePath), actions); err != nil {
		return errors.Annotate(err, "failed to write '%s'",
			actionsFile(w.cachePath))
	}
	w.metadata.NumActions = 0
	for _, as := range actions {
		w.metadata.NumActions += len(as)
	}
	return nil
}

// WritePrices saves the ticker prices to the DB file and incrementally updates
// the metadata.  Prices are assumed to be sorted by date.
func (w *Writer) WritePrices(ticker string, prices []PriceRow) error {
	if err := w.createDirs(); err != nil {
		return errors.Annotate(err, "failed to create DB directories")
	}
	if err := writeGob(pricesFile(w.cachePath, ticker), prices); err != nil {
		return errors.Annotate(err, "failed to write '%s'",
			pricesFile(w.cachePath, ticker))
	}
	w.metadata.NumPrices += len(prices)
	for _, p := range prices {
		if w.metadata.Start.IsZero() || w.metadata.Start.After(p.Date) {
			w.metadata.Start = p.Date
		}
		if w.metadata.End.IsZero() || w.metadata.End.Before(p.Date) {
			w.metadata.End = p.Date
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
func (w *Writer) WriteMonthly(monthly map[string][]ResampledRow) error {
	if err := w.createDirs(); err != nil {
		return errors.Annotate(err, "failed to create DB directories")
	}
	if err := writeGob(monthlyFile(w.cachePath), monthly); err != nil {
		return errors.Annotate(err, "failed to write '%s'",
			monthlyFile(w.cachePath))
	}
	w.metadata.NumMonthly = 0
	for _, ms := range monthly {
		w.metadata.NumMonthly += len(ms)
	}
	return nil
}

// WriteMetadata saves the metadata accumulated by the Write* methods. It is
// stored in JSON format to be human-readable.
func (w *Writer) WriteMetadata() error {
	fileName := metadataFile(w.cachePath)
	f, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return errors.Annotate(err, "failed to open file for writing: '%s'", fileName)
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(w.metadata); err != nil {
		return errors.Annotate(err, "failed to write to '%s'", fileName)
	}
	return nil
}

// DataConfig is the configuration of the data source.
type DataConfig struct {
	DBPath         string   `json:"DB path"`            // default: ~/.stockparfait
	DB             string   `json:"DB" required:"true"` // specific DB in path
	Tickers        []string `json:"tickers"`
	ExcludeTickers []string `json:"exclude tickers"`
	Exchanges      []string `json:"exchanges"`
	Names          []string `json:"names"`
	Categories     []string `json:"categories"`
	Sectors        []string `json:"sectors"`
	Industries     []string `json:"industries"`
	Start          Date     `json:"start"`
	End            Date     `json:"end"`
}

var _ message.Message = &DataConfig{}

// InitMessage implements message.Message.
func (d *DataConfig) InitMessage(js interface{}) error {
	if err := message.Init(d, js); err != nil {
		return err
	}
	if d.DBPath == "" {
		d.DBPath = filepath.Join(os.Getenv("HOME"), ".stockparfait")
	}
	return nil
}

// NewReaderFromConfig creates a new Reader from the config.
func NewReaderFromConfig(c *DataConfig) *Reader {
	r := NewReader(filepath.Join(c.DBPath, c.DB))
	r.Constraints.
		Ticker(c.Tickers...).
		ExcludeTicker(c.ExcludeTickers...).
		Exchange(c.Exchanges...).
		Name(c.Names...).
		Category(c.Categories...).
		Sector(c.Sectors...).
		Industry(c.Industries...).
		StartAt(c.Start).
		EndAt(c.End)

	return r
}
