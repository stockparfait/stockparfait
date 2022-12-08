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
	"math"
	"os"
	"path/filepath"
	"sync"

	"github.com/stockparfait/errors"
	"github.com/stockparfait/logging"
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

// Interval configures a set of constraints for a value over an optional time
// range. All interval ranges are inclusive, both value and time.
type Interval struct {
	Min   *float64 `json:"min"`
	Max   *float64 `json:"max"`
	Start Date     `json:"start"`
	End   Date     `json:"end"`
}

var _ message.Message = &Interval{}

func (i *Interval) InitMessage(js interface{}) error {
	return errors.Annotate(message.Init(i, js), "failed to init from JSON")
}

// ValueInRange checks if it belongs to the interval.
func (i Interval) ValueInRange(x float64) bool {
	if i.Min != nil && x < *i.Min {
		return false
	}
	if i.Max != nil && *i.Max < x {
		return false
	}
	return true
}

// DateInRange checks if it belongs to the date range of the interval.
func (i Interval) DateInRange(d Date) bool {
	return d.InRange(i.Start, i.End)
}

// Reader of the database, which also implements message.Message and can be used
// directly in a config.
type Reader struct {
	DBPath         string    `json:"DB path"`            // default: ~/.stockparfait
	DB             string    `json:"DB" required:"true"` // specific DB in path
	UseTickers     []string  `json:"tickers"`
	ExcludeTickers []string  `json:"exclude tickers"`
	Sources        []string  `json:"sources"`
	Exchanges      []string  `json:"exchanges"`
	Names          []string  `json:"names"`
	Categories     []string  `json:"categories"`
	Sectors        []string  `json:"sectors"`
	Industries     []string  `json:"industries"`
	Start          Date      `json:"start"`
	End            Date      `json:"end"`
	YearlyGrowth   *Interval `json:"yearly growth"`
	CashVolume     *Interval `json:"cash volume"`
	Volatility     *Interval `json:"volatility"`
	constraints    *Constraints
	tickers        map[string]TickerRow
	monthly        map[string][]ResampledRow
	metadata       Metadata
	tickersOnce    sync.Once
	tickersError   error
	monthlyOnce    sync.Once
	monthlyError   error
	metadataOnce   sync.Once
	metadataError  error
}

var _ message.Message = &Reader{}

func NewReader(dbPath, db string) *Reader {
	return &Reader{
		DBPath:  dbPath,
		DB:      db,
		tickers: make(map[string]TickerRow),
		monthly: make(map[string][]ResampledRow),
	}
}

func (r *Reader) initConstraints() {
	r.constraints = NewConstraints().
		Source(r.Sources...).
		Ticker(r.UseTickers...).
		ExcludeTicker(r.ExcludeTickers...).
		Exchange(r.Exchanges...).
		Name(r.Names...).
		Category(r.Categories...).
		Sector(r.Sectors...).
		Industry(r.Industries...)
}

// InitMessage implements message.Message.
func (r *Reader) InitMessage(js interface{}) error {
	if err := message.Init(r, js); err != nil {
		return errors.Annotate(err, "failed to init from JSON")
	}
	if r.DBPath == "" {
		r.DBPath = filepath.Join(os.Getenv("HOME"), ".stockparfait")
	}
	r.tickers = make(map[string]TickerRow)
	r.monthly = make(map[string][]ResampledRow)
	return nil
}

func tickersFile(cachePath string) string {
	return filepath.Join(cachePath, "tickers.gob")
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

func (r *Reader) cachePath() string {
	return filepath.Join(r.DBPath, r.DB)
}

func (r *Reader) cacheMetadata() error {
	r.metadataOnce.Do(func() {
		fileName := metadataFile(r.cachePath())
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
		if err := readGob(tickersFile(r.cachePath()), &r.tickers); err != nil {
			r.tickersError = errors.Annotate(
				err, "failed to load %s", tickersFile(r.cachePath()))
		}
	})
	return r.tickersError
}

func (r *Reader) cacheMonthly() error {
	r.monthlyOnce.Do(func() {
		if err := readGob(monthlyFile(r.cachePath()), &r.monthly); err != nil {
			r.monthlyError = errors.Annotate(
				err, "failed to load %s", monthlyFile(r.cachePath()))
		}
	})
	return r.monthlyError
}

func fileExists(fileName string) bool {
	info, err := os.Stat(fileName)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

// HasTickers checks if the DB exists and has the tickers table.
func (r *Reader) HasTickers() bool {
	return fileExists(tickersFile(r.cachePath()))
}

// HasMonthly checks if the DB exists and has the monthly table.
func (r *Reader) HasMonthly() bool {
	return fileExists(monthlyFile(r.cachePath()))
}

// HasPrices checks if the DB exists and has the prices for the ticker.
func (r *Reader) HasPrices(ticker string) bool {
	return fileExists(pricesFile(r.cachePath(), ticker))
}

// HasMetadata checks if the DB exists and has the metadata.
func (r *Reader) HasMetadata() bool {
	return fileExists(metadataFile(r.cachePath()))
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

func (r *Reader) growthInRange(ctx context.Context, ticker string) bool {
	if r.YearlyGrowth == nil {
		return true
	}
	monthly, err := r.Monthly(ticker, r.YearlyGrowth.Start, r.YearlyGrowth.End)
	if err != nil {
		logging.Warningf(ctx, "failed to load monthly data for %s:\n%s",
			ticker, err.Error())
		return false
	}
	if len(monthly) == 0 {
		return false
	}
	start := monthly[0].DateOpen
	end := monthly[len(monthly)-1].DateClose
	if start == end { // not enough data
		return false
	}
	first := monthly[0]
	last := monthly[len(monthly)-1]
	// Recompute open for full adjustment.
	open := float64(first.OpenSplitAdjusted) * float64(first.CloseFullyAdjusted) /
		float64(first.CloseSplitAdjusted)
	close := float64(last.CloseFullyAdjusted)
	years := start.YearsTill(end)
	growth := math.Exp((math.Log(close) - math.Log(open)) / years)
	return r.YearlyGrowth.ValueInRange(growth)
}

func (r *Reader) cashVolumeInRange(ctx context.Context, ticker string) bool {
	if r.CashVolume == nil {
		return true
	}
	monthly, err := r.Monthly(ticker, r.CashVolume.Start, r.CashVolume.End)
	if err != nil {
		logging.Warningf(ctx, "failed to load monthly data for %s:\n%s",
			ticker, err.Error())
		return false
	}
	if len(monthly) == 0 {
		return false
	}
	var total float32
	var samples uint16
	for _, m := range monthly {
		total += m.CashVolume
		samples += m.NumSamples
	}
	if samples == 0 {
		return false
	}
	avgVolume := float64(total) / float64(samples)
	return r.CashVolume.ValueInRange(avgVolume)
}

func (r *Reader) volatilityInRange(ctx context.Context, ticker string) bool {
	if r.Volatility == nil {
		return true
	}
	monthly, err := r.Monthly(ticker, r.Volatility.Start, r.Volatility.End)
	if err != nil {
		logging.Warningf(ctx, "failed to load monthly data for %s\n%s",
			ticker, err.Error())
		return false
	}
	if len(monthly) == 0 {
		return false
	}
	volatility, samples := DailyVolatility(monthly)
	if samples == 0 {
		return false
	}
	return r.Volatility.ValueInRange(volatility)
}

// checkTicker and its row if it satisfies all the constraints.
func (r *Reader) checkTicker(ctx context.Context, ticker string, row TickerRow) bool {
	if !r.constraints.CheckTicker(ticker) {
		return false
	}
	if !r.constraints.CheckTickerRow(row) {
		return false
	}
	return r.growthInRange(ctx, ticker) &&
		r.cashVolumeInRange(ctx, ticker) && r.volatilityInRange(ctx, ticker)
}

// Tickers returns the list of tickers satisfying current Reader's constraints.
// All tickers are cached in memory, and tickers are filtered for each call.
// Therefore, modifying Reader's constraints takes effect at the next call
// without re-reading the tickers.  Go-routine safe assuming constraints are not
// modified.
func (r *Reader) Tickers(ctx context.Context) ([]string, error) {
	if err := r.cacheTickers(); err != nil {
		return nil, errors.Annotate(err, "failed to load tickers")
	}
	r.initConstraints()
	tickers := []string{}
	for t, row := range r.tickers {
		if r.checkTicker(ctx, t, row) {
			tickers = append(tickers, t)
		}
	}
	return tickers, nil
}

// AllTickerRows returns all the ticker rows from the DB as a {ticker -> row}
// map, compatible with Writer.WriteTickers() method. Note: modifying the map
// will modify the Reader's cached copy.
func (r *Reader) AllTickerRows(ctx context.Context) (map[string]TickerRow, error) {
	if err := r.cacheTickers(); err != nil {
		return nil, errors.Annotate(err, "failed to load tickers")
	}
	return r.tickers, nil
}

// Prices for ticker satilfying Reader's constraints, sorted by date. Prices are
// NOT cached in memory, and are read from disk every time. It is probably go
// routine safe, if the underlying OS allows to open and read the same file
// multiple times from the same process. Reading different tickers is definitely
// safe in parallel, assuming consraints are not modified.
func (r *Reader) Prices(ticker string) ([]PriceRow, error) {
	prices := []PriceRow{}
	if err := readGob(pricesFile(r.cachePath(), ticker), &prices); err != nil {
		return nil, errors.Annotate(err, "failed to read prices for %s", ticker)
	}
	res := []PriceRow{}
	for _, p := range prices {
		if p.Date.InRange(r.Start, r.End) {
			res = append(res, p)
		}
	}
	return res, nil
}

// Monthly price data for ticker within the inclusive date range, sorted by
// date.  If any of start or end is zero value, the corresponding Reader
// constraint is used.  Data for all tickers are cached in memory upon the first
// call. Go routine safe assuming constraints are not modified.
func (r *Reader) Monthly(ticker string, start, end Date) ([]ResampledRow, error) {
	if err := r.cacheMonthly(); err != nil {
		return nil, errors.Annotate(err, "failed to load monthly data")
	}
	monthly, ok := r.monthly[ticker]
	if !ok {
		return nil, errors.Reason("no monthly data found for ticker %s", ticker)
	}
	if start.IsZero() {
		start = r.Start
	}
	if end.IsZero() {
		end = r.End
	}
	res := []ResampledRow{}
	for _, row := range monthly {
		if row.DateOpen.InRange(start, end) && row.DateClose.InRange(start, end) {
			res = append(res, row)
		}
	}
	return res, nil
}

// AllMonthlyRows returns all the monthly resampled rows from the DB as a
// {ticker -> row} map, compatible with Writer.WriteMonthly() method. Note:
// modifying the map will modify the Reader's cached copy.
func (r *Reader) AllMonthlyRows() (map[string][]ResampledRow, error) {
	if err := r.cacheMonthly(); err != nil {
		return nil, errors.Annotate(err, "failed to load tickers")
	}
	return r.monthly, nil
}

type Writer struct {
	dbPath     string
	db         string
	Metadata   Metadata
	mkdirOnce  sync.Once
	mkdirError error
}

func NewWriter(dbPath, db string) *Writer {
	return &Writer{dbPath: dbPath, db: db}
}

func (w *Writer) cachePath() string {
	return filepath.Join(w.dbPath, w.db)
}

func (w *Writer) createDirs() error {
	w.mkdirOnce.Do(func() {
		if err := os.MkdirAll(pricesDir(w.cachePath()), os.ModeDir|0755); err != nil {
			w.mkdirError = errors.Annotate(
				err, "failed to create %s", pricesDir(w.cachePath()))
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
	if err := writeGob(tickersFile(w.cachePath()), tickers); err != nil {
		return errors.Annotate(err, "failed to write '%s'", tickersFile(w.cachePath()))
	}
	w.Metadata.UpdateTickers(tickers)
	return nil
}

// WritePrices saves the ticker prices to the DB file and incrementally updates
// the metadata.  Prices are assumed to be sorted by date.
func (w *Writer) WritePrices(ticker string, prices []PriceRow) error {
	if err := w.createDirs(); err != nil {
		return errors.Annotate(err, "failed to create DB directories")
	}
	if err := writeGob(pricesFile(w.cachePath(), ticker), prices); err != nil {
		return errors.Annotate(err, "failed to write '%s'",
			pricesFile(w.cachePath(), ticker))
	}
	w.Metadata.UpdatePrices(prices)
	return nil
}

// ComputeMonthly converts daily price series into resampled monthly price
// series.
func ComputeMonthly(prices []PriceRow) []ResampledRow {
	if len(prices) == 0 {
		return nil
	}

	absLogProfit := func(x, y float32) float32 {
		if y <= 0.0 {
			return 0.0
		}
		diff := math.Log(float64(x)) - math.Log(float64(y))
		if diff < 0.0 {
			return -float32(diff)
		}
		return float32(diff)
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
				Open:              p.Close,
				OpenSplitAdjusted: p.CloseSplitAdjusted,
				OpenFullyAdjusted: p.CloseFullyAdjusted,
				DateOpen:          p.Date,
			}
		}
		currMonth = p.Date.MonthStart()
		currRes.Close = p.CloseUnadjusted()
		currRes.CloseSplitAdjusted = p.CloseSplitAdjusted
		currRes.CloseFullyAdjusted = p.CloseFullyAdjusted
		currRes.CashVolume += p.CashVolume
		currRes.DateClose = p.Date
		currRes.SumAbsLogProfits += absLogProfit(p.CloseFullyAdjusted, prevClose)
		currRes.NumSamples++
		currRes.Active = p.Active()
		prevClose = p.CloseFullyAdjusted
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
	if err := writeGob(monthlyFile(w.cachePath()), monthly); err != nil {
		return errors.Annotate(err, "failed to write '%s'",
			monthlyFile(w.cachePath()))
	}
	w.Metadata.UpdateMonthly(monthly)
	return nil
}

// WriteMetadata saves the metadata accumulated by the Write* methods. It is
// stored in JSON format to be human-readable.
func (w *Writer) WriteMetadata(m Metadata) error {
	fileName := metadataFile(w.cachePath())
	f, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return errors.Annotate(err, "failed to open file for writing: '%s'", fileName)
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(m); err != nil {
		return errors.Annotate(err, "failed to write to '%s'", fileName)
	}
	return nil
}
