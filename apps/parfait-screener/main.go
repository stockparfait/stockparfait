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

package main

import (
	"context"
	"flag"
	"io"
	"os"

	"github.com/stockparfait/errors"
	"github.com/stockparfait/logging"
	"github.com/stockparfait/stockparfait/message"
	"github.com/stockparfait/stockparfait/screener"
	"github.com/stockparfait/stockparfait/table"
)

type Flags struct {
	LogLevel logging.Level
	Config   string // config file
	CSV      bool   // dump CSV format; default: text.
}

func parseFlags(args []string) (*Flags, error) {
	var flags Flags
	fs := flag.NewFlagSet("parfait-screener", flag.ExitOnError)
	flags.LogLevel = logging.Info
	fs.Var(&flags.LogLevel, "log-level", "Log level: debug, info, warning, error")
	fs.StringVar(&flags.Config, "conf", "", "config file (required)")
	fs.BoolVar(&flags.CSV, "csv", false, "print table in CSV format; default: text")

	err := fs.Parse(args)
	if err != nil {
		return nil, err
	}
	if flags.Config == "" {
		return nil, errors.Reason("missing required -conf argument")
	}
	return &flags, err
}

func printData(ctx context.Context, flags *Flags, w io.Writer) error {
	var config screener.Config
	if err := message.FromFile(&config, flags.Config); err != nil {
		return errors.Annotate(err, "failed to read config '%s'", flags.Config)
	}
	tbl, err := screener.Screen(ctx, &config)
	if err != nil {
		return errors.Annotate(err, "failed to read data")
	}
	if flags.CSV {
		if err := tbl.WriteCSV(w, table.Params{}); err != nil {
			return errors.Annotate(err, "failed to print CSV")
		}
		return nil
	}
	if err := tbl.WriteText(w, table.Params{}); err != nil {
		return errors.Annotate(err, "failed to print text")
	}
	return nil
}

func main() {
	ctx := context.Background()
	flags, err := parseFlags(os.Args[1:])
	if err != nil {
		ctx = logging.Use(ctx, logging.DefaultGoLogger(logging.Info))
		logging.Errorf(ctx, "failed to parse flags: %s", err.Error())
		os.Exit(1)
	}
	ctx = logging.Use(ctx, logging.DefaultGoLogger(flags.LogLevel))

	if err := printData(ctx, flags, os.Stdout); err != nil {
		logging.Errorf(ctx, err.Error())
		os.Exit(1)
	}
}
