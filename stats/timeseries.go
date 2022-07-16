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

package stats

import (
	"math"

	"github.com/stockparfait/errors"
	"github.com/stockparfait/stockparfait/db"
)

// Timeseries stores numeric values along with timestamps. The timestamps are
// always sorted in ascending order.
type Timeseries struct {
	dates []db.Date
	data  []float64
}

// NewTimeseries creates a new empty Timeseries.
func NewTimeseries() *Timeseries {
	return &Timeseries{}
}

// Dates of the Timeseries.
func (t *Timeseries) Dates() []db.Date { return t.dates }

// Data of the Timeseries.
func (t *Timeseries) Data() []float64 { return t.data }

// Init assigns values to the Timeseries. The dates are expected to be sorted in
// ascending order (not checked). It returns self for inline declarations, and
// panics if the arguments don't have the same length.
func (t *Timeseries) Init(dates []db.Date, data []float64) *Timeseries {
	if len(dates) != len(data) {
		panic(errors.Reason("len(dates) [%d] != len(data) [%d]",
			len(dates), len(data)))
	}
	t.dates = dates
	t.data = data
	return t
}

// Copy makes a copy of the data before assigning it to the
// Timeseries. Otherwise it's the same as Init.
func (t *Timeseries) Copy(dates []db.Date, data []float64) *Timeseries {
	da := make([]db.Date, len(dates))
	dt := make([]float64, len(data))
	copy(da, dates)
	copy(dt, data)
	return t.Init(da, dt)
}

// Check that Timeseries is consistent: the lengths of dates and data are the
// same and the dates are ordered in ascending order.
func (t *Timeseries) Check() error {
	if len(t.dates) != len(t.data) {
		return errors.Reason("len(dates) [%d] != len(data) [%d]",
			len(t.dates), len(t.data))
	}
	for i, d := range t.dates {
		if i == 0 {
			continue
		}
		if !t.dates[i-1].Before(d) {
			return errors.Reason("dates[%d] = %s >= dates[%d] = %s",
				i-1, t.dates[i-1].String(), i, d.String())
		}
	}
	return nil
}

// rangeSlice returns slice indices for dates to extract an inclusive interval
// between start and end timestamps.
func rangeSlice(dates []db.Date, start, end db.Date) (s, e int) {
	if start.After(end) {
		return 0, 0
	}
	s = len(dates)
	e = len(dates)
	var startSet, endSet bool
	for i, d := range dates {
		if !startSet && !start.After(d) {
			s = i
			startSet = true
		}
		if !endSet && end.Before(d) {
			e = i
			endSet = true
		}
		if startSet && endSet {
			break
		}
	}
	if s >= e {
		return 0, 0
	}
	return
}

// Range extracts the sub-series from the inclusive time interval. It may return
// an empty Timeseries, but never nil.
func (t *Timeseries) Range(start, end db.Date) *Timeseries {
	s, e := rangeSlice(t.dates, start, end)
	if s == 0 && e == len(t.dates) {
		return t
	}
	return NewTimeseries().Init(t.dates[s:e], t.data[s:e])
}

// Shift the timeseries in time.  A positive shift moves the values into the
// future, negative - into the past. The values outside of the date range are
// dropped. It may return an empty Timeseries, but never nil.
func (t *Timeseries) Shift(shift int) *Timeseries {
	if shift == 0 {
		return t
	}
	absShift := shift
	if absShift < 0 {
		absShift = -shift
	}
	l := len(t.dates)
	if absShift >= l {
		return NewTimeseries()
	}
	if shift > 0 {
		return NewTimeseries().Init(t.dates[shift:], t.data[:l-shift])
	}
	return NewTimeseries().Init(t.dates[:l+shift], t.data[-shift:])
}

// DeltaParams are configuration parameters for computing delta time series. By
// default, d[t] = x[t] - x[t-1].
type DeltaParams struct {
	Relative   bool // d[t] = (x[t] - x[t-1]) / x[t-1]
	Log        bool // use log(x[t]) instead of x[t]
	Normalized bool // normalize deltas so mean=0 and MAD=1
}

// LogProfits computes a Sample of log-profits {log(x[t+1]) - log(x[t])}.
func (t *Timeseries) LogProfits() *Sample {
	data := make([]float64, len(t.Data()))
	for i, d := range t.Data() {
		data[i] = math.Log(d)
	}
	deltas := []float64{}
	for i := range data {
		if i == 0 {
			continue
		}
		deltas = append(deltas, data[i]-data[i-1])
	}
	return NewSample().Init(deltas)
}

// PriceField is an enum type indicating which PriceRow field to use.
type PriceField uint8

const (
	PriceUnadjusted PriceField = iota
	PriceSplitAdjusted
	PriceFullyAdjusted
	PriceCashVolume
)

// FromPrices initializes the Timeseries from PriceRow slice.
func (t *Timeseries) FromPrices(prices []db.PriceRow, f PriceField) *Timeseries {
	dates := make([]db.Date, len(prices))
	data := make([]float64, len(prices))
	for i, p := range prices {
		dates[i] = p.Date
		switch f {
		case PriceUnadjusted:
			data[i] = float64(p.CloseUnadjusted())
		case PriceSplitAdjusted:
			data[i] = float64(p.CloseSplitAdjusted)
		case PriceFullyAdjusted:
			data[i] = float64(p.CloseFullyAdjusted)
		case PriceCashVolume:
			data[i] = float64(p.CashVolume)
		default:
			panic(errors.Reason("unsupported PriceField: %d", f))
		}
	}
	return t.Init(dates, data)
}
