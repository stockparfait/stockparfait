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
	"github.com/stockparfait/stockparfait/ndl"
	"github.com/stockparfait/stockparfait/ndl/sharadar"

	toml "github.com/pelletier/go-toml/v2"
)

type Flags struct {
	DBDir    string // default: ~/.stockparfait/sharadar
	LogLevel logging.Level
}

func parseFlags(args []string) (*Flags, error) {
	var flags Flags
	fs := flag.NewFlagSet("sharadar", flag.ExitOnError)
	fs.StringVar(&flags.DBDir, "cache",
		filepath.Join(os.Getenv("HOME"), ".stockparfait", "sharadar"),
		"configuration path")
	flags.LogLevel = logging.Info
	fs.Var(&flags.LogLevel, "log-level", "Log level: debug, info, warning, error")

	err := fs.Parse(args)
	return &flags, err
}

type Config struct {
	Key    string               `toml:"key"`    // user key for Nasdaq Data Link
	Tables []sharadar.TableName `toml:"tables"` // which price tables to download
}

func parseConfig(dbdir string) (*Config, error) {
	filePath := filepath.Join(dbdir, "config.toml")
	if _, err := os.Stat(filePath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			sample := `key = "YourSecretNasdaqDataLinkKey"
tables = ["SEP", "SPF"]
`
			err = errors.Annotate(err,
				"config file '%s' does not exist.\nPlease create config file containing:\n%s",
				filePath, sample)
			return nil, err
		} else {
			return nil, errors.Annotate(err,
				"cannot check config file for existence: '%s'", filePath)
		}
	}
	f, err := os.Open(filePath)
	if err != nil {
		return nil, errors.Annotate(err, "failed to open config file %s", filePath)
	}
	defer f.Close()

	d := toml.NewDecoder(f)
	var c Config
	if err := d.Decode(&c); err != nil {
		return nil, errors.Annotate(err, "failed to read config file %s", filePath)
	}
	return &c, nil
}

func download(ctx context.Context, flags *Flags) error {
	config, err := parseConfig(flags.DBDir)
	if err != nil {
		return errors.Annotate(err, "failed to parse config")
	}

	ctx = ndl.UseClient(ctx, config.Key)
	ds := sharadar.NewDataset()
	if err := ds.DownloadAll(ctx, flags.DBDir, config.Tables...); err != nil {
		return errors.Annotate(err, "failed to download data")
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

	if err := download(ctx, flags); err != nil {
		logging.Errorf(ctx, err.Error())
		os.Exit(1)
	}
}
