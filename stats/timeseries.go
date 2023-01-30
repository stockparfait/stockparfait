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

// LogProfits computes a new Timeseries of log-profits {log(x[t+n]) -
// log(x[t])}. The associated log-profit date is t+n.
func (t *Timeseries) LogProfits(n int) *Timeseries {
	if n < 1 {
		panic(errors.Reason("n=%d must be >= 1", n))
	}
	data := make([]float64, len(t.Data()))
	for i, d := range t.Data() {
		data[i] = math.Log(d)
	}
	deltas := []float64{}
	for i := n; i < len(data); i++ {
		deltas = append(deltas, data[i]-data[i-n])
	}
	return NewTimeseries().Init(t.Dates()[n:], deltas)
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

// TimeseriesIntersectIndices returns the slice of indices S effectively
// intersecting the given Timeseries by Date. That is:
//
// - len(S) is the number of distinct Dates present in all of the tss;
//
// - len(S[i]) = len(tss) for any i<len(S), so each S[i] is the slice of indices
// in the corresponding Timeseries such that tss[j].Dates()[S[i][j]] ==
// tss[k].Dates()[S[i][k]] for any j, k < len(tss).
func TimeseriesIntersectIndices(tss ...*Timeseries) [][]int {
	var res [][]int
	if len(tss) == 0 {
		return res
	}
	curr := make([]int, len(tss)) // current set of indices into Timeseries
	var currDate db.Date
	done := false // there are no more common Dates

	for !done {
		match := true // found a common Date in all of tss

		for i := 0; i < len(tss); i++ {
			if curr[i] >= len(tss[i].Dates()) {
				done = true
				match = false
				break
			}
			if currDate.IsZero() {
				currDate = tss[i].Dates()[curr[i]]
			}
			for curr[i] < len(tss[i].Dates()) && tss[i].Dates()[curr[i]].Before(currDate) {
				curr[i]++
			}
			if curr[i] >= len(tss[i].Dates()) {
				done = true
				match = false
				break
			}
			if tss[i].Dates()[curr[i]] != currDate {
				currDate = tss[i].Dates()[curr[i]]
				match = false
				break
			}
		}
		if match {
			cp := make([]int, len(curr))
			copy(cp, curr)
			res = append(res, cp)
			currDate = db.Date{}
			for j := 0; j < len(curr); j++ {
				curr[j]++
			}
		}
	}
	return res
}

// TimeseriesIntersect creates new list of Timeseries whose Dates are identical
// by dropping the mismatching Dates and Data elements out. The resulting slice
// is guaranteed to be of the same length as the number of arguments and contain
// valid Timeseries, even if they are empty.
func TimeseriesIntersect(tss ...*Timeseries) []*Timeseries {
	if len(tss) == 0 {
		return nil
	}
	res := make([]*Timeseries, len(tss))
	for i := 0; i < len(res); i++ {
		res[i] = NewTimeseries()
	}
	ind := TimeseriesIntersectIndices(tss...)
	if len(ind) == 0 {
		return res
	}
	dates := make([]db.Date, len(ind))
	for j := 0; j < len(ind); j++ {
		dates[j] = tss[0].Dates()[ind[j][0]]
	}
	for i := 0; i < len(res); i++ {
		data := make([]float64, len(ind))
		for j := 0; j < len(ind); j++ {
			data[j] = tss[i].Data()[ind[j][i]]
		}
		res[i].Init(dates, data)
	}
	return res
}
