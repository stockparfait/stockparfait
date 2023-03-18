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
	"sort"
	"strconv"
	"strings"

	"github.com/stockparfait/errors"
	"github.com/stockparfait/stockparfait/message"
)

// TickerRowConfig sets the custom headers of input CSV file for ticker rows.
type TickerRowConfig struct {
	Ticker      string   `json:"Ticker" default:"Ticker"`
	Source      string   `json:"Source" default:"Source"`
	Exchange    string   `json:"Exchange" default:"Exchange"`
	Name        string   `json:"Name" default:"Name"`
	Category    string   `json:"Category" default:"Category"`
	Sector      string   `json:"Sector" default:"Sector"`
	Industry    string   `json:"Industry" default:"Industry"`
	Location    string   `json:"Location" default:"Location"`
	SECFilings  string   `json:"SEC Filings" default:"SEC Filings"`
	CompanySite string   `json:"Company Site" default:"Company Site"`
	Active      string   `json:"Active" default:"Active"`
	Header      []string `json:"header"` // for headless CSV
}

var _ message.Message = &TickerRowConfig{}

// InitMessage implements message.Message.
func (c *TickerRowConfig) InitMessage(js any) error {
	return errors.Annotate(message.Init(c, js), "failed to init from JSON")
}

func NewTickerRowConfig() *TickerRowConfig {
	var c TickerRowConfig
	if err := c.InitMessage(map[string]any{}); err != nil {
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

// priceRaw stores values loaded from a CSV row.  A zero value means the value
// is missing. The final PriceRow may need to recover some of the actual DB
// values from these values.
type priceRaw struct {
	Date   Date
	Open   float32
	High   float32
	Low    float32
	Close  float32
	Volume float32
	// Split adjusted.
	OpenSplitAdjusted   float32
	HighSplitAdjusted   float32
	LowSplitAdjusted    float32
	CloseSplitAdjusted  float32
	VolumeSplitAdjusted float32
	// Fully adjusted.
	OpenFullyAdjusted   float32
	HighFullyAdjusted   float32
	LowFullyAdjusted    float32
	CloseFullyAdjusted  float32
	VolumeFullyAdjusted float32
	// Adjustment independent values.
	CashVolume float32
	Active     bool
}

func (p *priceRaw) ToPriceRow() PriceRow {
	var pr PriceRow
	pr.Date = p.Date
	pr.Close = p.Close
	pr.CloseSplitAdjusted = p.CloseSplitAdjusted
	pr.CloseFullyAdjusted = p.CloseFullyAdjusted
	var splitAdj, fullAdj float32
	if p.CloseSplitAdjusted != 0 {
		splitAdj = p.Close / p.CloseSplitAdjusted
	}
	if p.CloseFullyAdjusted != 0 {
		fullAdj = p.Close / p.CloseFullyAdjusted
	}
	pr.Open = p.Open
	if p.Open == 0 {
		if p.OpenSplitAdjusted != 0 && splitAdj != 0 {
			pr.Open = p.OpenSplitAdjusted * splitAdj
		} else if p.OpenFullyAdjusted != 0 && fullAdj != 0 {
			pr.Open = p.OpenFullyAdjusted * fullAdj
		}
	}
	pr.High = p.High
	if p.High == 0 {
		if p.HighSplitAdjusted != 0 && splitAdj != 0 {
			pr.High = p.HighSplitAdjusted * splitAdj
		} else if p.HighFullyAdjusted != 0 && fullAdj != 0 {
			pr.High = p.HighFullyAdjusted * fullAdj
		}
	}
	pr.Low = p.Low
	if p.Low == 0 {
		if p.LowSplitAdjusted != 0 && splitAdj != 0 {
			pr.Low = p.LowSplitAdjusted * splitAdj
		} else if p.LowFullyAdjusted != 0 && fullAdj != 0 {
			pr.Low = p.LowFullyAdjusted * fullAdj
		}
	}
	pr.CashVolume = p.CashVolume
	if p.CashVolume == 0 {
		if p.VolumeSplitAdjusted != 0 && p.CloseSplitAdjusted != 0 {
			pr.CashVolume = p.VolumeSplitAdjusted * p.CloseSplitAdjusted
		} else if p.VolumeFullyAdjusted != 0 && p.CloseFullyAdjusted != 0 {
			pr.CashVolume = p.VolumeFullyAdjusted * p.CloseFullyAdjusted
		}
	}
	pr.SetActive(p.Active)
	return pr
}

// PriceRowConfig sets the custom headers of input CSV file for price rows.  The
// mapping is one-to-many; in particular, several price fields can map to the
// same CSV column.
//
// Note, that the DB stores natively only unadjusted OHLC, split- and fully
// adjusted Close prices, and cash volume defined as close*volume in shares.
// When other fields are configured and imported, such as adjusted OHL and
// adjusted or unadjusted volume (in shares), the actual values stored in the DB
// are derived from the ratios of the corresponding Close prices.
type PriceRowConfig struct {
	// Unadjusted
	Date   string `json:"Date" default:"Date"`
	Open   string `json:"Open" default:"Open"`
	High   string `json:"High" default:"High"`
	Low    string `json:"Low" default:"Low"`
	Close  string `json:"Close" default:"Close"`
	Volume string `json:"Volume"`
	// Split adjusted
	OpenSplitAdjusted   string `json:"Open split adj"`
	HighSplitAdjusted   string `json:"High split adj"`
	LowSplitAdjusted    string `json:"Low split adj"`
	CloseSplitAdjusted  string `json:"Close split adj" default:"Close split adj"`
	VolumeSplitAdjusted string `json:"Volume split adj"`
	// Split & divided adjusted
	OpenFullyAdjusted   string `json:"Open fully adj"`
	HighFullyAdjusted   string `json:"High fully adj"`
	LowFullyAdjusted    string `json:"Low fully adj"`
	CloseFullyAdjusted  string `json:"Close fully adj" default:"Close fully adj"`
	VolumeFullyAdjusted string `json:"Volume fully adj"`
	// Adjustment independent values
	CashVolume string   `json:"Cash Volume" default:"Cash Volume"`
	Active     string   `json:"Active" default:"Active"`
	Header     []string `json:"header"` // for headless CSV
}

var _ message.Message = &PriceRowConfig{}

func (c *PriceRowConfig) InitMessage(js any) error {
	return errors.Annotate(message.Init(c, js), "failed to init from JSON")
}

func NewPriceRowConfig() *PriceRowConfig {
	var c PriceRowConfig
	if err := c.InitMessage(map[string]any{}); err != nil {
		panic(errors.Annotate(err, "failed to init default PriceRowConfig"))
	}
	return &c
}

// HasPrice checks that the header contains at least one closing price column.
func (c *PriceRowConfig) HasPrice(header []string) bool {
	for _, h := range header {
		if h == c.Close || h == c.CloseSplitAdjusted || h == c.CloseFullyAdjusted {
			return true
		}
	}
	return false
}

const (
	priceDate int = iota
	priceOpen
	priceHigh
	priceLow
	priceClose
	priceVolume
	priceOpenSplitAdjusted
	priceHighSplitAdjusted
	priceLowSplitAdjusted
	priceCloseSplitAdjusted
	priceVolumeSplitAdjusted
	priceOpenFullyAdjusted
	priceHighFullyAdjusted
	priceLowFullyAdjusted
	priceCloseFullyAdjusted
	priceVolumeFullyAdjusted
	priceCashVolume
	priceActive
	priceLast // keep it last; not a real value.
)

// MapColumns maps the i'th header column to the list of PriceRow fields. In
// particular, a single price column can map to several price fields in
// PriceRow.
func (c *PriceRowConfig) MapColumns(header []string) [][]int {
	m := make([][]int, len(header))
	cols := make([]string, priceLast)
	cols[priceDate] = c.Date
	cols[priceOpen] = c.Open
	cols[priceHigh] = c.High
	cols[priceLow] = c.Low
	cols[priceClose] = c.Close
	cols[priceVolume] = c.Volume
	cols[priceOpenSplitAdjusted] = c.OpenSplitAdjusted
	cols[priceHighSplitAdjusted] = c.HighSplitAdjusted
	cols[priceLowSplitAdjusted] = c.LowSplitAdjusted
	cols[priceCloseSplitAdjusted] = c.CloseSplitAdjusted
	cols[priceVolumeSplitAdjusted] = c.VolumeSplitAdjusted
	cols[priceOpenFullyAdjusted] = c.OpenFullyAdjusted
	cols[priceHighFullyAdjusted] = c.HighFullyAdjusted
	cols[priceLowFullyAdjusted] = c.LowFullyAdjusted
	cols[priceCloseFullyAdjusted] = c.CloseFullyAdjusted
	cols[priceVolumeFullyAdjusted] = c.VolumeFullyAdjusted
	cols[priceCashVolume] = c.CashVolume
	cols[priceActive] = c.Active
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
	p := priceRaw{Active: true}
	for i, r := range row {
		if i >= len(colMap) {
			break
		}
		for _, field := range colMap[i] {
			var v float64
			switch field {
			case priceDate:
				p.Date, err = NewDateFromString(r)
				if err != nil {
					err = errors.Annotate(err, "failed to parse date")
					return
				}
			case priceOpen:
				v, err = strconv.ParseFloat(r, 32)
				if err != nil {
					err = errors.Annotate(err, "failed to parse unadjusted Open: %s", r)
					return
				}
				p.Open = float32(v)
			case priceHigh:
				v, err = strconv.ParseFloat(r, 32)
				if err != nil {
					err = errors.Annotate(err, "failed to parse unadjusted High: %s", r)
					return
				}
				p.High = float32(v)
			case priceLow:
				v, err = strconv.ParseFloat(r, 32)
				if err != nil {
					err = errors.Annotate(err, "failed to parse unadjusted Low: %s", r)
					return
				}
				p.Low = float32(v)
			case priceClose:
				v, err = strconv.ParseFloat(r, 32)
				if err != nil {
					err = errors.Annotate(err, "failed to parse unadjusted Close: %s", r)
					return
				}
				p.Close = float32(v)
			case priceVolume:
				v, err = strconv.ParseFloat(r, 32)
				if err != nil {
					err = errors.Annotate(err, "failed to parse unadjusted Volume: %s", r)
					return
				}
				p.Volume = float32(v)
			case priceOpenSplitAdjusted:
				v, err = strconv.ParseFloat(r, 32)
				if err != nil {
					err = errors.Annotate(err, "failed to parse split-adjusted Open: %s", r)
					return
				}
				p.OpenSplitAdjusted = float32(v)
			case priceHighSplitAdjusted:
				v, err = strconv.ParseFloat(r, 32)
				if err != nil {
					err = errors.Annotate(err, "failed to parse split-adjusted High: %s", r)
					return
				}
				p.HighSplitAdjusted = float32(v)
			case priceLowSplitAdjusted:
				v, err = strconv.ParseFloat(r, 32)
				if err != nil {
					err = errors.Annotate(err, "failed to parse split-adjusted Low: %s", r)
					return
				}
				p.LowSplitAdjusted = float32(v)
			case priceCloseSplitAdjusted:
				v, err = strconv.ParseFloat(r, 32)
				if err != nil {
					err = errors.Annotate(err, "failed to parse split-adjusted Close: %s", r)
					return
				}
				p.CloseSplitAdjusted = float32(v)
			case priceVolumeSplitAdjusted:
				v, err = strconv.ParseFloat(r, 32)
				if err != nil {
					err = errors.Annotate(err, "failed to parse split-adjusted Volume: %s", r)
					return
				}
				p.VolumeSplitAdjusted = float32(v)
			case priceOpenFullyAdjusted:
				v, err = strconv.ParseFloat(r, 32)
				if err != nil {
					err = errors.Annotate(err, "failed to parse fully adjusted Open: %s", r)
					return
				}
				p.OpenFullyAdjusted = float32(v)
			case priceHighFullyAdjusted:
				v, err = strconv.ParseFloat(r, 32)
				if err != nil {
					err = errors.Annotate(err, "failed to parse fully adjusted High: %s", r)
					return
				}
				p.HighFullyAdjusted = float32(v)
			case priceLowFullyAdjusted:
				v, err = strconv.ParseFloat(r, 32)
				if err != nil {
					err = errors.Annotate(err, "failed to parse fully adjusted Low: %s", r)
					return
				}
				p.LowFullyAdjusted = float32(v)
			case priceCloseFullyAdjusted:
				v, err = strconv.ParseFloat(r, 32)
				if err != nil {
					err = errors.Annotate(err, "failed to parse fully adjusted Close: %s", r)
					return
				}
				p.CloseFullyAdjusted = float32(v)
			case priceVolumeFullyAdjusted:
				v, err = strconv.ParseFloat(r, 32)
				if err != nil {
					err = errors.Annotate(err, "failed to parse fully adjusted Volume: %s", r)
					return
				}
				p.VolumeFullyAdjusted = float32(v)
			case priceCashVolume:
				v, err = strconv.ParseFloat(r, 32)
				if err != nil {
					err = errors.Annotate(err, "failed to parse cash volume: %s", r)
					return
				}
				p.CashVolume = float32(v)
			case priceActive:
				p.Active = str2bool(r)
			}
		}
	}
	pr = p.ToPriceRow()
	return
}

// ReadCSVTickers reads raw CSV and updates the tickers table compatible with DB
// Writer.
//
// When config defines a header, CSV is assumed to be headless; otherwise the
// CSV file must have a header. In either case, the header must contain a Ticker
// column.  Columns with an unrecognized header are ignored, missing columns are
// assumed to be of their default values, which are empty for all strings and
// true for Active.
func ReadCSVTickers(r io.Reader, c *TickerRowConfig, tickers map[string]TickerRow) error {
	csvReader := csv.NewReader(r)
	rows, err := csvReader.ReadAll()
	if err != nil {
		return errors.Annotate(err, "failed to read tickers from CSV")
	}
	if len(rows) <= 1 {
		return nil
	}
	header := c.Header
	if len(header) == 0 {
		header = rows[0]
		rows = rows[1:]
	}
	if !c.HasTicker(header) {
		return errors.Reason("tickers CSV requires a Ticker column")
	}
	colMap := c.MapColumns(header)
	for _, r := range rows {
		ticker, tr := c.Parse(r, colMap)
		tickers[ticker] = tr
	}
	return nil
}

// ReadCSVPrices reads raw CSV and creates a price series compatible with DB
// Writer.
//
// When config defines a header, CSV is assumed to be headless; otherwise the
// CSV file must have a header. In either case, the header must contain at least
// one type of price. The remaining prices can be set by mapping them to the
// same column name.  Columns with an unrecognized header are ignored, missing
// columns are set to 0.
func ReadCSVPrices(r io.Reader, c *PriceRowConfig) ([]PriceRow, error) {
	csvReader := csv.NewReader(r)
	rows, err := csvReader.ReadAll()
	if err != nil {
		return nil, errors.Annotate(err, "failed to read prices from CSV")
	}
	if len(rows) <= 1 {
		return nil, nil
	}
	header := c.Header
	if len(header) == 0 {
		header = rows[0]
		rows = rows[1:]
	}
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
	sort.Slice(prices, func(i, j int) bool { return prices[i].Date.Before(prices[j].Date) })
	return prices, nil
}
