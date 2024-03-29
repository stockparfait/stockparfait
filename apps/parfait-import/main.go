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
	"encoding/json"
	"flag"
	"io"
	"math"
	"os"
	"path/filepath"

	"github.com/stockparfait/errors"
	"github.com/stockparfait/logging"
	"github.com/stockparfait/stockparfait/db"
)

type Flags struct {
	DBDir    string // default: ~/.stockparfait
	DBName   string // required
	LogLevel logging.Level
	// Exactly one of tickers, prices or update-metadata must be present
	Tickers        string // Import tickers; merge by default
	Replace        bool   // Replace tickers table rather than merge
	Ticker         string // Must be present with -prices
	Prices         string // Import prices for a given ticker
	Schema         string // schema file for either tickers or prices table
	UpdateMetadata bool
	Cleanup        bool
}

func parseFlags(args []string) (*Flags, error) {
	var flags Flags
	fs := flag.NewFlagSet("parfait-import", flag.ExitOnError)
	fs.StringVar(&flags.DBDir, "cache",
		filepath.Join(os.Getenv("HOME"), ".stockparfait"),
		"path to databases")
	fs.StringVar(&flags.DBName, "db", "", "database name (required)")
	flags.LogLevel = logging.Info
	fs.Var(&flags.LogLevel, "log-level", "Log level: debug, info, warning, error")
	fs.StringVar(&flags.Tickers, "tickers", "", "import tickers")
	fs.BoolVar(&flags.Replace, "replace", false,
		"replace the entire tickers table, don't merge")
	fs.StringVar(&flags.Ticker, "ticker", "", "required with -prices")
	fs.StringVar(&flags.Prices, "prices", "", "import prices for a given ticker")
	fs.StringVar(&flags.Schema, "schema", "", "schema config for either tickers or prices")
	fs.BoolVar(&flags.UpdateMetadata, "update-metadata", false, "scan the DB")
	fs.BoolVar(&flags.Cleanup, "cleanup", false, "clean up orphan price files")

	err := fs.Parse(args)
	if err != nil {
		return nil, err
	}
	if flags.DBName == "" {
		return nil, errors.Reason("missing required -db argument")
	}
	kinds := 0
	if flags.Tickers != "" {
		kinds++
	}
	if flags.Prices != "" {
		kinds++
	}
	if flags.UpdateMetadata {
		kinds++
	}
	if flags.Cleanup {
		kinds++
	}
	if kinds != 1 {
		return nil, errors.Reason(
			"expected exactly one of -tickers, -prices, -update-metadata or -cleanup")
	}
	if flags.Prices != "" && flags.Ticker == "" {
		return nil, errors.Reason("-ticker is required with -prices")
	}
	return &flags, err
}

func readJSON(fileName string) (any, error) {
	f, err := os.Open(fileName)
	if err != nil {
		return nil, errors.Annotate(err, "cannot open config file '%s'", fileName)
	}
	defer f.Close()

	dec := json.NewDecoder(f)
	var js any
	if err := dec.Decode(&js); err != nil && err != io.EOF {
		return nil, errors.Annotate(err, "failed to decode JSON in '%s'", fileName)
	}
	return js, nil
}

func importTickers(ctx context.Context, flags *Flags) error {
	tickers := make(map[string]db.TickerRow)
	if !flags.Replace {
		r := db.NewReader(flags.DBDir, flags.DBName)
		if r.HasTickers() {
			var err error
			tickers, err = r.AllTickerRows()
			if err != nil {
				return errors.Annotate(err, "failed to read existing tickers")
			}
		}
	}
	c := db.NewTickerRowConfig()
	if flags.Schema != "" {
		js, err := readJSON(flags.Schema)
		if err != nil {
			return errors.Annotate(err, "failed to read config")
		}
		if err := c.InitMessage(js); err != nil {
			return errors.Annotate(err, "failed to init tickers schema")
		}
	}
	f, err := os.Open(flags.Tickers)
	if err != nil {
		return errors.Annotate(err, "cannot open tickers file '%s'", flags.Tickers)
	}
	defer f.Close()

	if err := db.ReadCSVTickers(f, c, tickers); err != nil {
		return errors.Annotate(err, "failed to read tickers from '%s'", flags.Tickers)
	}

	w := db.NewWriter(flags.DBDir, flags.DBName)
	if err := w.WriteTickers(tickers); err != nil {
		return errors.Annotate(err, "failed to write tickers to DB")
	}
	logging.Infof(ctx, "imported %d tickers", len(tickers))
	return nil
}

func hasInf(p db.PriceRow) bool {
	for _, v := range []float32{p.Close, p.CloseSplitAdjusted, p.CloseFullyAdjusted, p.CashVolume} {
		if math.IsInf(float64(v), 0) {
			return true
		}
	}
	return false
}

func hasNaN(p db.PriceRow) bool {
	for _, v := range []float32{p.Close, p.CloseSplitAdjusted, p.CloseFullyAdjusted, p.CashVolume} {
		if math.IsNaN(float64(v)) {
			return true
		}
	}
	return false
}

func updateMonthly(ctx context.Context, flags *Flags, prices []db.PriceRow) error {
	r := db.NewReader(flags.DBDir, flags.DBName)
	monthly := make(map[string][]db.ResampledRow)
	if r.HasMonthly() {
		var err error
		monthly, err = r.AllMonthlyRows()
		if err != nil {
			return errors.Annotate(err, "failed to read existing monthly data")
		}
	}
	monthly[flags.Ticker] = db.ComputeMonthly(prices)
	w := db.NewWriter(flags.DBDir, flags.DBName)
	if err := w.WriteMonthly(monthly); err != nil {
		return errors.Annotate(err, "failed to save updated monthly data")
	}
	logging.Infof(ctx, "wrote %d monthly samples for %s",
		len(monthly[flags.Ticker]), flags.Ticker)
	return nil
}

func importPrices(ctx context.Context, flags *Flags) error {
	c := db.NewPriceRowConfig()
	if flags.Schema != "" {
		js, err := readJSON(flags.Schema)
		if err != nil {
			return errors.Annotate(err, "failed to read config")
		}
		if err := c.InitMessage(js); err != nil {
			return errors.Annotate(err, "failed to init prices config")
		}
	}
	f, err := os.Open(flags.Prices)
	if err != nil {
		return errors.Annotate(err, "cannot open prices file '%s'", flags.Prices)
	}
	defer f.Close()

	pricesRaw, err := db.ReadCSVPrices(f, c)
	if err != nil {
		return errors.Annotate(err, "failed to read prices from '%s'", flags.Prices)
	}

	// Filter out rows with invalid values.
	var nans, infs, noDates int
	var prices []db.PriceRow
	for _, p := range pricesRaw {
		if p.Date.IsZero() {
			noDates++
			continue
		}
		if hasInf(p) {
			infs++
			continue
		}
		if hasNaN(p) {
			nans++
			continue
		}
		prices = append(prices, p)
	}
	if nans > 0 || infs > 0 || noDates > 0 {
		logging.Warningf(ctx,
			"ignored %d rows with no date, %d rows with NaN and %d rows with Inf values out of total %d rows",
			noDates, nans, infs, len(pricesRaw))
	}
	if len(prices) == 0 {
		return errors.Reason("there are no prices to import")
	}
	w := db.NewWriter(flags.DBDir, flags.DBName)
	if err := w.WritePrices(flags.Ticker, prices); err != nil {
		return errors.Annotate(err, "failed to write prices for %s to DB", flags.Ticker)
	}
	logging.Infof(ctx, "imported %d prices to %s", len(prices), flags.Ticker)
	if err := updateMonthly(ctx, flags, prices); err != nil {
		return errors.Annotate(err,
			"failed to update monthly prices for %s", flags.Ticker)
	}
	return nil
}

func updateMetadata(ctx context.Context, flags *Flags) error {
	r := db.NewReader(flags.DBDir, flags.DBName)
	if !r.HasTickers() {
		return errors.Reason("no tickers found in DB %s", flags.DBName)
	}
	tickers, err := r.AllTickerRows()
	if err != nil {
		return errors.Annotate(err, "failed to read tickers from %s", flags.DBName)
	}
	var m db.Metadata
	for t := range tickers {
		prices, err := r.Prices(t)
		if err != nil {
			logging.Warningf(ctx, "no prices for %s, skipping", t)
			continue
		}
		m.UpdatePrices(prices)
		m.NumTickers++
	}
	monthly, err := r.AllMonthlyRows()
	if err != nil {
		return errors.Annotate(err, "failed to read monthly data from %s",
			flags.DBName)
	}
	m.UpdateMonthly(monthly)
	w := db.NewWriter(flags.DBDir, flags.DBName)
	if err := w.WriteMetadata(m); err != nil {
		return errors.Annotate(err, "failed to write metadata to %s", flags.DBName)
	}
	logging.Infof(ctx, "updated metadata")
	return nil
}

func run(args []string) error {
	flags, err := parseFlags(args)
	if err != nil {
		return errors.Annotate(err, "failed to parse flags")
	}
	ctx := context.Background()
	ctx = logging.Use(ctx, logging.DefaultGoLogger(flags.LogLevel))
	if flags.Tickers != "" {
		return errors.Annotate(importTickers(ctx, flags),
			"failed to import tickers from '%s'", flags.Tickers)
	}
	if flags.Prices != "" {
		return errors.Annotate(importPrices(ctx, flags),
			"failed to import prices for %s from '%s'", flags.Ticker, flags.Prices)
	}
	if flags.UpdateMetadata {
		return errors.Annotate(updateMetadata(ctx, flags),
			"failed to update metadata")
	}
	if flags.Cleanup {
		return errors.Annotate(db.Cleanup(ctx, flags.DBDir, flags.DBName),
			"failed to clean up DB")
	}
	return nil
}

// main is not tested, keep it short.
func main() {
	if err := run(os.Args[1:]); err != nil {
		ctx := context.Background()
		ctx = logging.Use(ctx, logging.DefaultGoLogger(logging.Info))
		logging.Errorf(ctx, err.Error())
		os.Exit(1)
	}
}
