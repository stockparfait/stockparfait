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
	"flag"
	"os"
	"path/filepath"

	"github.com/stockparfait/errors"
	"github.com/stockparfait/logging"
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
	if kinds != 1 {
		return nil, errors.Reason(
			"expected exactly one of -tickers, -prices or -update-metadata")
	}
	if flags.Prices != "" && flags.Ticker == "" {
		return nil, errors.Reason("-ticker is required with -prices")
	}
	return &flags, err
}

func run(args []string) error {
	flags, err := parseFlags(args)
	if err != nil {
		return errors.Annotate(err, "failed to parse flags")
	}
	ctx := context.Background()
	ctx = logging.Use(ctx, logging.DefaultGoLogger(flags.LogLevel))
	logging.Infof(ctx, "not yet implemented")
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
