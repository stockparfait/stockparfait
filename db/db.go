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
	"os"

	"github.com/stockparfait/errors"
)

type contextKey int

const (
	dbContextKey contextKey = iota
)

// UseDB injects database directory path into the context.
func UseDB(ctx context.Context, db *Database) context.Context {
	return context.WithValue(ctx, dbContextKey, db)
}

// GetDB extracts database directory path from the context.
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

// ResampledFrequency is enum for the available resample rates.
type ResampledFrequency int

// ResampledFrequency values.
const (
	Weekly ResampledFrequency = iota
	Monthly
	Quarterly
)

type Database struct {
	cachePath string
	tickers   map[string]TickerRow
	weekly    map[string][]ResampledRow
	monthly   map[string][]ResampledRow
	quarterly map[string][]ResampledRow
}

func NewDatabase(cachePath string) *Database {
	return &Database{
		cachePath: cachePath,
		tickers:   make(map[string]TickerRow),
		weekly:    make(map[string][]ResampledRow),
		monthly:   make(map[string][]ResampledRow),
		quarterly: make(map[string][]ResampledRow),
	}
}
