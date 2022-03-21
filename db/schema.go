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
	"fmt"
	"time"

	"github.com/stockparfait/errors"
)

// lessLex is a lexicographic ordering on the slices of int.
func lessLex(x, y []int) bool {
	l := len(x)
	if len(y) < l {
		l = len(y)
	}
	for i := 0; i < l; i++ {
		if x[i] < y[i] {
			return true
		}
		if x[i] > y[i] {
			return false
		}
	}
	return len(x) < len(y)
}

// Date records a calendar date as year, month and day. The struct is designed
// to fit into 4 bytes.
type Date struct {
	YearVal  uint16
	MonthVal uint8
	DayVal   uint8
}

// NewDate is the constructor for Date.
func NewDate(year uint16, month, day uint8) Date {
	return Date{year, month, day}
}

// NewDateFromTime creates a Date instance from a Time value in UTC.
func NewDateFromTime(t time.Time) Date {
	return Date{
		YearVal:  uint16(t.Year()),
		MonthVal: uint8(t.Month()),
		DayVal:   uint8(t.Day()),
	}
}

// DateInNY returns today's date in New York timezone.
func DateInNY(now time.Time) Date {
	tz := "America/New_York"
	location, err := time.LoadLocation(tz)
	if err != nil {
		panic(errors.Annotate(err, "failed to load timezone %s", tz))
	}
	t := now.In(location)
	return NewDateFromTime(t)
}

func (d Date) Year() uint16 { return d.YearVal }
func (d Date) Month() uint8 { return d.MonthVal }
func (d Date) Day() uint8   { return d.DayVal }

// String representation of the value.
func (d Date) String() string {
	return fmt.Sprintf("%04d-%02d-%02d", d.Year(), d.Month(), d.Day())
}

// ToTime converts Date to Time in UTC.
func (d Date) ToTime() time.Time {
	return time.Date(int(d.Year()), time.Month(d.Month()), int(d.Day()), 0, 0, 0, 0, time.UTC)
}

// Monday return a new Date of the Monday of the current date's week.
func (d Date) Monday() Date {
	t := d.ToTime()
	t = t.AddDate(0, 0, 1-int(t.Weekday()))
	return NewDateFromTime(t)
}

// MonthStart returns the 1st of the month of the current date.
func (d Date) MonthStart() Date {
	return NewDate(d.Year(), d.Month(), 1)
}

// QuarterStart returns the first day of the quarter of the current date.
func (d Date) QuarterStart() Date {
	return NewDate(d.Year(), (d.Month()-1)/3*3+1, 1)
}

// Before compares two Date objects for strict inequality (self < d2).
func (d Date) Before(d2 Date) bool {
	return lessLex([]int{int(d.Year()), int(d.Month()), int(d.Day())},
		[]int{int(d2.Year()), int(d2.Month()), int(d2.Day())})
}

// After compares two Date objects for strict inequality, self > d2.
func (d Date) After(d2 Date) bool {
	return d2.Before(d)
}

// IsZero checks whether the date has a zero value.
func (d Date) IsZero() bool {
	return d.Year() == 0 && d.Month() == 0 && d.Day() == 0
}

// MaxDate returns the latest date from the list, or nil if the list is empty.
func MaxDate(dates ...Date) Date {
	var max Date
	for _, d := range dates {
		if max.IsZero() || max.Before(d) {
			max = d
		}
	}
	return max
}

// TickerRow is a row in the tickers table.
type TickerRow struct {
	Exchange    string // the primary exchange trading this ticker
	Name        string // the company name
	Category    string
	Sector      string
	Industry    string
	Location    string
	SECFiling   string // URL
	CompanySite string // URL
}

// ActionRow is a row in the actions table. Size: 16 bytes (13+padding).
type ActionRow struct {
	Date           Date
	DividendFactor float32 // dividend adjustment factor (1.0 = no dividend)
	SplitFactor    float32 // split adjustment factor (1.0 = no split)
	Active         bool
}

// TestAction creates an ActionRow for use in tests.
func TestAction(date Date, dividend, split float32, active bool) ActionRow {
	return ActionRow{
		Date:           date,
		DividendFactor: dividend,
		SplitFactor:    split,
		Active:         active,
	}
}

// PriceRow is a row in the prices table. It is intended for daily price points.
// Size: 24 bytes.
type PriceRow struct {
	Date         Date
	Open         float32
	High         float32
	Low          float32
	Close        float32
	DollarVolume float32
}

// TestPrice creates a PriceRow instance for use in tests.
func TestPrice(date Date, close, dv float32) PriceRow {
	return PriceRow{
		Date:         date,
		Open:         close,
		High:         close,
		Low:          close,
		Close:        close,
		DollarVolume: dv,
	}
}

// ResampledRow is a multi-day bar with some additional daily statistics.  Size:
// 48 bytes (46+padding).
type ResampledRow struct {
	Open         float32 // split-adjusted prices
	High         float32
	Low1         float32 // low before high
	Low2         float32 // low at or after high
	Close        float32
	DollarVolume float32
	DateOpen     Date
	DateHigh     Date
	DateClose    Date
	Dividends    float32 // split-adjusted dollar dividends
	// Sum of relative daily movements within the bar: sum(|p(t+1)-p(t)|/p(t)).
	// Note, that it always has NumSamples-1 samples.
	SumRelativeMove float32
	NumSamples      uint16
	Active          bool // if ticker is active at bar's close
}

// TestResampled creates a new ResampledRow for use in tests.
func TestResampled(dateOpen, dateClose Date, open, high, low, close, dv float32, active bool) ResampledRow {
	return ResampledRow{
		Open:            open,
		High:            high,
		Low1:            low,
		Low2:            low,
		Close:           close,
		DollarVolume:    dv,
		DateOpen:        dateOpen,
		DateHigh:        dateOpen,
		DateClose:       dateClose,
		Dividends:       0.0,
		SumRelativeMove: 10.0,
		NumSamples:      1000.0,
		Active:          active,
	}
}
