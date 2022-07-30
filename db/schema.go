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
	"encoding/json"
	"fmt"
	"time"

	"github.com/stockparfait/errors"
	"github.com/stockparfait/stockparfait/message"
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

func parseTime(s string) (time.Time, error) {
	if s == "0000-00-00" || s == "0000-00-00T00:00:00.000" {
		return time.Time{}, nil
	}
	formats := []string{
		"2006-01-02 15:04:05.999",
		"2006-01-02T15:04:05.999",
		"2006-01-02T15:04:05.999Z",
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
		"2006-01-02",
	}
	var err error
	for _, fmt := range formats {
		var tm time.Time
		tm, err := time.Parse(fmt, s)
		if err == nil {
			return tm, nil
		}
	}
	return time.Time{}, err
}

// Date records a calendar date as year, month and day. The struct is designed
// to fit into 4 bytes.
type Date struct {
	YearVal  uint16
	MonthVal uint8
	DayVal   uint8
}

var _ json.Marshaler = Date{}
var _ json.Unmarshaler = &Date{}
var _ message.Message = &Date{}

// NewDate is the constructor for Date.
func NewDate(year uint16, month, day uint8) Date {
	return Date{year, month, day}
}

// NewDateFromTime creates a Date instance from a time.Time value in UTC.
func NewDateFromTime(t time.Time) Date {
	return Date{
		YearVal:  uint16(t.Year()),
		MonthVal: uint8(t.Month()),
		DayVal:   uint8(t.Day()),
	}
}

// NewDateFromString creates a Date instance from a string representation.
func NewDateFromString(s string) (Date, error) {
	t, err := parseTime(s)
	if err != nil {
		return Date{}, errors.Annotate(err, "failed to parse a Date string: '%s'", s)
	}
	return NewDateFromTime(t), nil
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

// MarshalJSON implements json.Marshaler.
func (d Date) MarshalJSON() ([]byte, error) {
	return []byte(`"` + d.String() + `"`), nil
}

// UnmarshalJSON implements json.Unmarshaler. NOTE: unlike other methods, this
// is a pointer method.
func (d *Date) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return errors.Annotate(err, "Date JSON must be a string")
	}
	date, err := NewDateFromString(s)
	if err != nil {
		return errors.Annotate(err, "failed to parse Date string")
	}
	*d = date
	return nil
}

// InitMessage implements message.Message.
func (d *Date) InitMessage(js interface{}) error {
	switch s := js.(type) {
	case string:
		date, err := NewDateFromString(s)
		if err != nil {
			return errors.Annotate(err, "failed to parse Date string")
		}
		*d = date
	case map[string]interface{}:
		*d = Date{}
	default:
		return errors.Reason("expected a string or {}, got %v", js)
	}
	return nil
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

// MinDate returns the earliest date from the list, or zero value.
func MinDate(dates ...Date) Date {
	var min Date
	for _, d := range dates {
		if min.IsZero() || (!d.IsZero() && min.After(d)) {
			min = d
		}
	}
	return min
}

// MaxDate returns the latest date from the list, or zero value.
func MaxDate(dates ...Date) Date {
	var max Date
	for _, d := range dates {
		if max.IsZero() || (!d.IsZero() && max.Before(d)) {
			max = d
		}
	}
	return max
}

func (d Date) IsLeapYear() bool {
	if d.Year()%400 == 0 {
		return true
	}
	if d.Year()%100 == 0 {
		return false
	}
	if d.Year()%4 == 0 {
		return true
	}
	return false
}

// DaysInMonth is the numder of days in the current month, which for February
// depends on the year.
func (d Date) DaysInMonth() uint8 {
	if d.IsZero() {
		return 0
	}
	switch d.Month() {
	case 1, 3, 5, 7, 8, 10, 12:
		return 31
	case 4, 6, 9, 11:
		return 30
	case 2:
		if d.IsLeapYear() {
			return 29
		} else {
			return 28
		}
	}
	return 0
}

// YearsTill returns possibly fractional number of years between the two dates.
func (d Date) YearsTill(d2 Date) float64 {
	if d.IsZero() || d2.IsZero() {
		return 0.0
	}
	years := float64(d2.Year()) - float64(d.Year())
	years += (float64(d2.Month()) - float64(d.Month())) / 12.0
	years += (float64(d2.Day())/float64(d2.DaysInMonth()) - float64(d.Day())/float64(d.DaysInMonth())) / 12.0
	return years
}

// InRange checks if d is in the inclusive date range. Any of the bounds may be
// zero value, in which case it's ignored.
func (d Date) InRange(start, end Date) bool {
	if d.IsZero() {
		return false
	}
	if !start.IsZero() && start.After(d) {
		return false
	}
	if !end.IsZero() && end.Before(d) {
		return false
	}
	return true
}

// TickerRow is a row in the tickers table.
type TickerRow struct {
	Exchange    string // the primary exchange trading this ticker
	Name        string // the company name
	Category    string
	Sector      string
	Industry    string
	Location    string
	SECFilings  string // URL
	CompanySite string // URL
	Active      bool   // ticker is listed at the last price date
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
// Size: 20 bytes.
//
// Note, that the sign of the (unadjusted) Close price indicates whether the
// ticker is listed at the close of the day (negate = delisted). Therefore, use
// CloseUnadjusted() and Active() methods to get the corresponding values.
//
// Price and cash volume are assumed to be in the stock's native currency,
// e.g. dollar for the US stocks.
type PriceRow struct {
	Date               Date
	Close              float32 // unadjusted; negative means delisted
	CloseSplitAdjusted float32 // adjusted only for splits
	CloseFullyAdjusted float32 // adjusted for splits, dividends, spinoffs
	CashVolume         float32 // shares volume * closing price
}

// CloseUnadjusted price, separated from the activity status.
func (p *PriceRow) CloseUnadjusted() float32 {
	if p.Close < 0.0 {
		return -p.Close
	} else {
		return p.Close
	}
}

// Active indicates whether the ticker is currently listed.
func (p *PriceRow) Active() bool {
	return p.Close > 0.0
}

// SetActive bit for the price point.
func (p *PriceRow) SetActive(active bool) {
	if p.Active() != active {
		p.Close = -p.Close
	}
}

// TestPrice creates a PriceRow instance for use in tests.
func TestPrice(date Date, close, adj, dv float32, active bool) PriceRow {
	p := PriceRow{
		Date:               date,
		Close:              close,
		CloseSplitAdjusted: adj,
		CloseFullyAdjusted: adj,
		CashVolume:         dv,
	}
	p.SetActive(active)
	return p
}

// ResampledRow is a multi-day bar with some additional daily statistics.  Size:
// 36 bytes.
type ResampledRow struct {
	OpenSplitAdjusted  float32
	Close              float32 // unadjusted
	CloseSplitAdjusted float32 // adjusted only for splits
	CloseFullyAdjusted float32 // adjusted for splits, dividends, spinoffs
	CashVolume         float32
	DateOpen           Date
	DateClose          Date
	// Sum of relative daily movements within the bar: sum(|p(t+1)-p(t)|/p(t)).
	// Note, that it always has NumSamples-1 samples. When summing over multiple
	// resampled bars, recover the missing sample as (open(t)-close(t-1))/open(t).
	SumRelativeMove float32
	NumSamples      uint16
	Active          bool // if ticker is active at bar's close
}

// TestResampled creates a new ResampledRow for use in tests.
func TestResampled(dateOpen, dateClose Date, open, close, adj, dv float32, active bool) ResampledRow {
	return ResampledRow{
		OpenSplitAdjusted:  open,
		Close:              close,
		CloseSplitAdjusted: adj,
		CloseFullyAdjusted: adj,
		CashVolume:         dv,
		DateOpen:           dateOpen,
		DateClose:          dateClose,
		SumRelativeMove:    0.2,
		NumSamples:         20,
		Active:             active,
	}
}

// Metadata is the schema for the metadata.json file.
type Metadata struct {
	Start      Date `json:"start"` // the earliest available price date
	End        Date `json:"end"`   // the latest available price date
	NumTickers int  `json:"num_tickers"`
	NumActions int  `json:"num_actions"`
	NumPrices  int  `json:"num_prices"`  // daily price samples
	NumMonthly int  `json:"num_monthly"` // monthly price samples
}

// Time is a wrapper around time.Time with JSON methods.
type Time time.Time

var _ json.Marshaler = &Time{}
var _ json.Unmarshaler = &Time{}

func NewTime(year, month, day, hour, minute, second int) *Time {
	t := time.Date(year, time.Month(month), day, hour, minute, second, 0, time.UTC)
	return (*Time)(&t)
}

// String representation of Time.
func (t *Time) String() string {
	return time.Time(*t).Format("2006-01-02 15:04:05")
}

// MarshalJSON implements json.Marshaler.
func (t *Time) MarshalJSON() ([]byte, error) {
	return []byte(`"` + t.String() + `"`), nil
}

// UnmarshalJSON implements json.Unmarshaler.
func (t *Time) UnmarshalJSON(data []byte) error {
	var s string
	var err error
	if err = json.Unmarshal(data, &s); err != nil {
		return errors.Annotate(err, "Time JSON must be a string")
	}
	tm, err := parseTime(s)
	if err != nil {
		return errors.Annotate(err, "failed to parse time string: '%s'", s)
	}
	*t = Time(tm)
	return nil
}
