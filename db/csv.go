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
	"encoding/csv"
	"io"
	"strconv"
	"strings"

	"github.com/stockparfait/errors"
	"github.com/stockparfait/stockparfait/message"
)

// TickerRowConfig sets the custom headers of input CSV file for ticker rows.
type TickerRowConfig struct {
	Ticker      string `json:"Ticker" default:"Ticker"`
	Source      string `json:"Source" default:"Source"`
	Exchange    string `json:"Exchange" default:"Exchange"`
	Name        string `json:"Name" default:"Name"`
	Category    string `json:"Category" default:"Category"`
	Sector      string `json:"Sector" default:"Sector"`
	Industry    string `json:"Industry" default:"Industry"`
	Location    string `json:"Location" default:"Location"`
	SECFilings  string `json:"SEC Filings" default:"SEC Filings"`
	CompanySite string `json:"Company Site" default:"Company Site"`
	Active      string `json:"Active" default:"Active"`
}

var _ message.Message = &TickerRowConfig{}

// InitMessage implements message.Message.
func (c *TickerRowConfig) InitMessage(js interface{}) error {
	return errors.Annotate(message.Init(c, js), "failed to init from JSON")
}

func NewTickerRowConfig() *TickerRowConfig {
	var c TickerRowConfig
	if err := c.InitMessage(map[string]interface{}{}); err != nil {
		panic(errors.Annotate(err, "failed to init default TickerRowConfig"))
	}
	return &c
}

// HasTicker checks the header for the column corresponding to the ticker name.
func (c *TickerRowConfig) HasTicker(header []string) bool {
	for _, h := range header {
		if h == c.Ticker {
			return true
		}
	}
	return false
}

// MapColumns maps the i'th header column to the j'th TickerTableRow field.
// Headers that don't match any configured column are mapped to -1.
func (c *TickerRowConfig) MapColumns(header []string) []int {
	m := make([]int, len(header))
	cols := []string{
		c.Ticker,
		c.Source,
		c.Exchange,
		c.Name,
		c.Category,
		c.Sector,
		c.Industry,
		c.Location,
		c.SECFilings,
		c.CompanySite,
		c.Active,
	}
	for i, h := range header {
		m[i] = -1
		for j, n := range cols {
			if h == n {
				m[i] = j
				break
			}
		}
	}
	return m
}

func str2bool(s string) bool {
	switch strings.ToLower(s) {
	case "t", "true", "y", "yes":
		return true
	}
	return false
}

func (c *TickerRowConfig) Parse(row []string, colMap []int) (ticker string, tr TickerRow) {
	tr.Active = true
	for i, r := range row {
		if i >= len(colMap) {
			break
		}
		switch colMap[i] {
		case 0:
			ticker = r
		case 1:
			tr.Source = r
		case 2:
			tr.Exchange = r
		case 3:
			tr.Name = r
		case 4:
			tr.Category = r
		case 5:
			tr.Sector = r
		case 6:
			tr.Industry = r
		case 7:
			tr.Location = r
		case 8:
			tr.SECFilings = r
		case 9:
			tr.CompanySite = r
		case 10:
			tr.Active = str2bool(r)
		}
	}
	return
}

// PriceRowConfig sets the custom headers of input CSV file for price rows.  The
// mapping is one-to-many; in particular, several price fields can map to the
// same CSV column.
type PriceRowConfig struct {
	Date               string `json:"Date" default:"Date"`
	Close              string `json:"Close" default:"Close"`
	CloseSplitAdjusted string `json:"Close split adj" default:"Close split adj"`
	CloseFullyAdjusted string `json:"Close fully adj" default:"Close fully adj"`
	CashVolume         string `json:"Cash Volume" default:"Cash Volume"`
	Active             string `json:"Active" default:"Active"`
}

var _ message.Message = &PriceRowConfig{}

func (c *PriceRowConfig) InitMessage(js interface{}) error {
	return errors.Annotate(message.Init(c, js), "failed to init from JSON")
}

func NewPriceRowConfig() *PriceRowConfig {
	var c PriceRowConfig
	if err := c.InitMessage(map[string]interface{}{}); err != nil {
		panic(errors.Annotate(err, "failed to init default PriceRowConfig"))
	}
	return &c
}

// HasPrice checks that the header contains at least one price column.
func (c *PriceRowConfig) HasPrice(header []string) bool {
	for _, h := range header {
		if h == c.Close || h == c.CloseSplitAdjusted || h == c.CloseFullyAdjusted {
			return true
		}
	}
	return false
}

// MapColumns maps the i'th header column to the list of PriceRow fields. In
// particular, a single price column can map to several price fields in
// PriceRow.
func (c *PriceRowConfig) MapColumns(header []string) [][]int {
	m := make([][]int, len(header))
	cols := []string{
		c.Date,
		c.Close,
		c.CloseSplitAdjusted,
		c.CloseFullyAdjusted,
		c.CashVolume,
		c.Active,
	}
	for i, h := range header {
		for j, n := range cols {
			if h == n {
				m[i] = append(m[i], j)
			}
		}
	}
	return m
}

func (c *PriceRowConfig) Parse(row []string, colMap [][]int) (pr PriceRow, err error) {
	active := true
	for i, r := range row {
		if i >= len(colMap) {
			break
		}
		for _, field := range colMap[i] {
			var v float64
			switch field {
			case 0:
				pr.Date, err = NewDateFromString(r)
				if err != nil {
					err = errors.Annotate(err, "failed to parse date")
					return
				}
			case 1:
				v, err = strconv.ParseFloat(r, 32)
				if err != nil {
					err = errors.Annotate(err, "failed to parse unadjusted Close: %s", r)
					return
				}
				pr.Close = float32(v)
			case 2:
				v, err = strconv.ParseFloat(r, 32)
				if err != nil {
					err = errors.Annotate(err, "failed to parse split-adjusted Close: %s", r)
					return
				}
				pr.CloseSplitAdjusted = float32(v)
			case 3:
				v, err = strconv.ParseFloat(r, 32)
				if err != nil {
					err = errors.Annotate(err, "failed to parse fully adjusted Close: %s", r)
					return
				}
				pr.CloseFullyAdjusted = float32(v)
			case 4:
				v, err = strconv.ParseFloat(r, 32)
				if err != nil {
					err = errors.Annotate(err, "failed to parse cash volume: %s", r)
					return
				}
				pr.CashVolume = float32(v)
			case 5:
				active = str2bool(r)
			}
		}
	}
	pr.SetActive(active)
	return
}

// ReadCSVTickers reads raw CSV and creates a tickers table compatible with DB
// Writer.
//
// The CSV file must have a header and at the minimum contain the Ticker column.
// Columns with an unrecognized header are ignored, missing columns are assumed
// to be of their default values, which are empty for all strings and true for
// Active.
func ReadCSVTickers(r io.Reader, c *TickerRowConfig) (map[string]TickerRow, error) {
	csvReader := csv.NewReader(r)
	rows, err := csvReader.ReadAll()
	if err != nil {
		return nil, errors.Annotate(err, "failed to read tickers from CSV")
	}
	if len(rows) <= 1 {
		return nil, nil
	}
	header := rows[0]
	rows = rows[1:]
	if !c.HasTicker(header) {
		return nil, errors.Reason("tickers CSV requires a Ticker column")
	}
	colMap := c.MapColumns(header)
	tickers := make(map[string]TickerRow)
	for _, r := range rows {
		ticker, tr := c.Parse(r, colMap)
		tickers[ticker] = tr
	}
	return tickers, nil
}

// ReadCSVPrices reads raw CSV and creates a price series compatible with DB
// Writer.
//
// The CSV file must have a header and contain at least one type of price. The
// remaining prices can be set by mapping them to the same column name.  Columns
// with an unrecognized header are ignored, missing columns are set to 0.
func ReadCSVPrices(r io.Reader, c *PriceRowConfig) ([]PriceRow, error) {
	csvReader := csv.NewReader(r)
	rows, err := csvReader.ReadAll()
	if err != nil {
		return nil, errors.Annotate(err, "failed to read prices from CSV")
	}
	if len(rows) <= 1 {
		return nil, nil
	}
	header := rows[0]
	rows = rows[1:]
	if !c.HasPrice(header) {
		return nil, errors.Reason("prices CSV requires at least one price column")
	}
	colMap := c.MapColumns(header)
	prices := []PriceRow{}
	for i, r := range rows {
		pr, err := c.Parse(r, colMap)
		if err != nil {
			return nil, errors.Annotate(err, "failed to parse row %d", i)
		}
		prices = append(prices, pr)
	}
	return prices, nil
}
