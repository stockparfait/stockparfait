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
	"math"
	"time"

	"github.com/stockparfait/errors"
	"github.com/stockparfait/stockparfait/message"
	"github.com/stockparfait/stockparfait/table"
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

// Date records a calendar date and time as year, month, day and millisecond
// from the midnight. The struct is designed to fit into 8 bytes.
type Date struct {
	YearVal  uint16
	MonthVal uint8
	DayVal   uint8
	MsecVal  uint32 // milliseconds from midnight
}

var _ json.Marshaler = Date{}
var _ json.Unmarshaler = &Date{}
var _ message.Message = &Date{}

// NewDate is a Date constructor without time of the day.
func NewDate(year uint16, month, day uint8) Date {
	return Date{YearVal: year, MonthVal: month, DayVal: day}
}

// NewDatetime is a Date constructor with time of the day.
func NewDatetime(year uint16, month, day, hour, minute, second uint8, msec uint32) Date {
	return Date{
		YearVal:  year,
		MonthVal: month,
		DayVal:   day,
		MsecVal:  (uint32(hour)*3600+uint32(minute)*60+uint32(second))*1000 + uint32(msec),
	}
}

// NewDateFromTime creates a Date instance from a time.Time value in UTC.
func NewDateFromTime(t time.Time) Date {
	var zero time.Time
	if t == zero {
		return Date{}
	}
	return Date{
		YearVal:  uint16(t.Year()),
		MonthVal: uint8(t.Month()),
		DayVal:   uint8(t.Day()),
		MsecVal:  uint32(((t.Hour()*60+t.Minute())*60+t.Second())*1000 + t.Nanosecond()/1000000),
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

func (d Date) Year() uint16        { return d.YearVal }
func (d Date) Month() uint8        { return d.MonthVal }
func (d Date) Day() uint8          { return d.DayVal }
func (d Date) Hour() uint8         { return uint8(d.MsecVal / 3600000) }
func (d Date) Minute() uint8       { return uint8((d.MsecVal % 3600000) / 60000) }
func (d Date) Second() uint8       { return uint8((d.MsecVal % 60000) / 1000) }
func (d Date) Millisecond() uint32 { return d.MsecVal % 1000 }

// String representation of the value.
func (d Date) String() string {
	if d.MsecVal == 0 {
		return fmt.Sprintf("%04d-%02d-%02d", d.Year(), d.Month(), d.Day())
	}
	return fmt.Sprintf("%04d-%02d-%02dT%02d:%02d:%02d.%03d",
		d.Year(), d.Month(), d.Day(), d.Hour(), d.Minute(), d.Second(), d.Millisecond())
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
func (d *Date) InitMessage(js any) error {
	switch s := js.(type) {
	case string:
		date, err := NewDateFromString(s)
		if err != nil {
			return errors.Annotate(err, "failed to parse Date string")
		}
		*d = date
	case map[string]any:
		*d = Date{}
	default:
		return errors.Reason("expected a string or {}, got %v", js)
	}
	return nil
}

// ToTime converts Date to Time in UTC.
func (d Date) ToTime() time.Time {
	return time.Date(int(d.Year()), time.Month(d.Month()), int(d.Day()),
		int(d.Hour()), int(d.Minute()), int(d.Second()),
		int(d.Millisecond()*1000000), time.UTC)
}

// Date returns the date value without time of day (MsecVal = 0).
func (d Date) Date() Date {
	return NewDate(d.Year(), d.Month(), d.Day())
}

// Monday returns a new Date of the Monday midnight of the current date's
// week. Note: week is assumed to start on Sunday, so Monday(d=Sunday) returns
// the next day.
func (d Date) Monday() Date {
	t := d.ToTime()
	t = t.AddDate(0, 0, 1-int(t.Weekday()))
	return NewDate(uint16(t.Year()), uint8(t.Month()), uint8(t.Day()))
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
	return lessLex([]int{int(d.Year()), int(d.Month()), int(d.Day()), int(d.MsecVal)},
		[]int{int(d2.Year()), int(d2.Month()), int(d2.Day()), int(d2.MsecVal)})
}

// After compares two Date objects for strict inequality, self > d2.
func (d Date) After(d2 Date) bool {
	return d2.Before(d)
}

// IsZero checks whether the date has a zero value.
func (d Date) IsZero() bool {
	return d.Year() == 0 && d.Month() == 0 && d.Day() == 0 && d.MsecVal == 0
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
	years += (float64(d2.MsecVal) - float64(d.MsecVal)) / (365.25 * 1000)
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
	Source      string // where the ticker was downloaded, e.g. NDL table name
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

// Row converts TickerRow to table.Row which includes the ticker name.
func (t TickerRow) Row(ticker string) table.Row {
	return TickerTableRow{
		TickerRow: t,
		Ticker:    ticker,
	}
}

// TickerTableRow implements table.Row by adding ticker name to TickerRow.
type TickerTableRow struct {
	TickerRow
	Ticker string
}

var _ table.Row = TickerTableRow{}

func (r TickerTableRow) CSV() []string {
	return []string{
		r.Ticker,
		r.Source,
		r.Exchange,
		r.Name,
		r.Category,
		r.Sector,
		r.Industry,
		r.Location,
		r.SECFilings,
		r.CompanySite,
		bool2str(r.Active),
	}
}

// TickerRowHeader for CSV table.
func TickerRowHeader() []string {
	return []string{
		"Ticker",
		"Source",
		"Exchange",
		"Name",
		"Category",
		"Sector",
		"Industry",
		"Location",
		"SEC Filings",
		"Company Site",
		"Active",
	}
}

func bool2str(x bool) string {
	if x {
		return "TRUE"
	}
	return "FALSE"
}

func float2str(x float32) string {
	return fmt.Sprintf("%g", x)
}

func uint2str(x uint16) string {
	return fmt.Sprintf("%d", x)
}

// PriceRow is a row in the prices table. It is intended for daily price points.
// Size: 32 bytes.
//
// Note, that the sign of the (unadjusted) Close price indicates whether the
// ticker is listed at the close of the day (negate = delisted). Therefore, use
// CloseUnadjusted() and Active() methods to get the corresponding values.
//
// Price and cash volume are assumed to be in the stock's native currency,
// e.g. dollar for the US stocks.
type PriceRow struct {
	Date               Date
	Open               float32 // all other prices are unadjusted
	High               float32
	Low                float32
	Close              float32 // unadjusted; negative means delisted
	CloseSplitAdjusted float32 // adjusted only for splits
	CloseFullyAdjusted float32 // adjusted for splits, dividends, spinoffs
	CashVolume         float32 // shares volume * closing price
}

var _ table.Row = PriceRow{}

func PriceRowHeader() []string {
	return []string{
		"Date",
		"Open",
		"High",
		"Low",
		"Close",
		"Close split adj",
		"Close fully adj",
		"Cash Volume",
		"Active",
	}
}

func (p PriceRow) CSV() []string {
	return []string{
		p.Date.String(),
		float2str(p.Open),
		float2str(p.High),
		float2str(p.Low),
		float2str(p.CloseUnadjusted()),
		float2str(p.CloseSplitAdjusted),
		float2str(p.CloseFullyAdjusted),
		float2str(p.CashVolume),
		bool2str(p.Active()),
	}
}

func (p PriceRow) OpenFullyAdjusted() float32 {
	if p.Close == 0 {
		return 0
	}
	return p.Open / p.CloseUnadjusted() * p.CloseFullyAdjusted
}

func (p PriceRow) OpenSplitAdjusted() float32 {
	if p.Close == 0 {
		return 0
	}
	return p.Open / p.CloseUnadjusted() * p.CloseSplitAdjusted
}

func (p PriceRow) HighFullyAdjusted() float32 {
	if p.Close == 0 {
		return 0
	}
	return p.High / p.CloseUnadjusted() * p.CloseFullyAdjusted
}

func (p PriceRow) HighSplitAdjusted() float32 {
	if p.Close == 0 {
		return 0
	}
	return p.High / p.CloseUnadjusted() * p.CloseSplitAdjusted
}

func (p PriceRow) LowFullyAdjusted() float32 {
	if p.Close == 0 {
		return 0
	}
	return p.Low / p.CloseUnadjusted() * p.CloseFullyAdjusted
}

func (p PriceRow) LowSplitAdjusted() float32 {
	if p.Close == 0 {
		return 0
	}
	return p.Low / p.CloseUnadjusted() * p.CloseSplitAdjusted
}

// CloseUnadjusted price, separated from the activity status.
func (p PriceRow) CloseUnadjusted() float32 {
	if p.Close < 0.0 {
		return -p.Close
	} else {
		return p.Close
	}
}

// Active indicates whether the ticker is currently listed.
func (p PriceRow) Active() bool {
	return !math.Signbit(float64(p.Close))
}

// SetActive bit for the price point.
func (p *PriceRow) SetActive(active bool) {
	if p.Active() != active {
		p.Close = -p.Close
	}
}

// TestPrice creates a PriceRow instance for use in tests. It uses the closing
// price to assign the other OHL prices.
func TestPrice(date Date, close, splitAdj, fullyAdj, dv float32, active bool) PriceRow {
	return TestPriceRow(date, close, close, close, close, splitAdj, fullyAdj, dv, active)
}

// TestPriceRow is a complete version of TestPrice which allows to set all OHLC
// prices directly. The OHL prices are always fully adjusted.
func TestPriceRow(date Date, open, high, low, close, closeSplitAdj, closeFullyAdj, dv float32, active bool) PriceRow {
	p := PriceRow{
		Date:               date,
		Open:               open,
		High:               high,
		Low:                low,
		Close:              close,
		CloseSplitAdjusted: closeSplitAdj,
		CloseFullyAdjusted: closeFullyAdj,
		CashVolume:         dv,
	}
	p.SetActive(active)
	return p
}

// ResampledRow is a multi-day bar with some additional daily statistics.  Size:
// 44 bytes.
type ResampledRow struct {
	Open               float32
	OpenSplitAdjusted  float32
	OpenFullyAdjusted  float32
	Close              float32 // unadjusted
	CloseSplitAdjusted float32 // adjusted only for splits
	CloseFullyAdjusted float32 // adjusted for splits, dividends, spinoffs
	CashVolume         float32
	DateOpen           Date
	DateClose          Date
	// Sum of absolute values of daily log-profits within the bar:
	// sum(abs(log(p(t+1))-log(p(t)))).  Note, that it always has NumSamples-1
	// samples. When summing over multiple resampled bars, recover the missing
	// sample as abs(log(open(t))-log(close(t-1))).
	SumAbsLogProfits float32
	NumSamples       uint16
	Active           bool // if ticker is active at bar's close
}

var _ table.Row = &ResampledRow{}

func (r ResampledRow) CSV() []string {
	return []string{
		float2str(r.Open),
		float2str(r.OpenSplitAdjusted),
		float2str(r.OpenFullyAdjusted),
		float2str(r.Close),
		float2str(r.CloseSplitAdjusted),
		float2str(r.CloseFullyAdjusted),
		float2str(r.CashVolume),
		r.DateOpen.String(),
		r.DateClose.String(),
		float2str(r.SumAbsLogProfits),
		uint2str(r.NumSamples),
		bool2str(r.Active),
	}
}

func ResampledRowHeader() []string {
	return []string{
		"Open",
		"Open split adj",
		"Open fully adj",
		"Close",
		"Close split adj",
		"Close fully adj",
		"Cash Volume",
		"Date Open",
		"Date Close",
		"Sum Abs Log Profits",
		"Samples",
		"Active",
	}
}

// TestResampled creates a new ResampledRow for use in tests.
func TestResampled(dateOpen, dateClose Date, open, close, adj, dv float32, active bool) ResampledRow {
	return ResampledRow{
		Open:               open,
		OpenSplitAdjusted:  open,
		OpenFullyAdjusted:  open,
		Close:              close,
		CloseSplitAdjusted: adj,
		CloseFullyAdjusted: adj,
		CashVolume:         dv,
		DateOpen:           dateOpen,
		DateClose:          dateClose,
		SumAbsLogProfits:   0.2,
		NumSamples:         20,
		Active:             active,
	}
}

// DailyVolatility computes the average daily absolute log-profit from the list
// of consecutive monthly bars.
func DailyVolatility(rows []ResampledRow) (volatility float64, samples uint16) {
	absLogProfit := func(x, y float32) float64 {
		diff := math.Log(float64(x)) - math.Log(float64(y))
		if diff < 0.0 {
			return -diff
		}
		return diff
	}

	var total float64
	var prevRow ResampledRow
	for i, r := range rows {
		if i > 0 && r.OpenFullyAdjusted > 0.0 && prevRow.CloseFullyAdjusted > 0.0 {
			total += absLogProfit(r.OpenFullyAdjusted, prevRow.CloseFullyAdjusted)
			samples++
		}
		total += float64(r.SumAbsLogProfits)
		samples += r.NumSamples - 1
		prevRow = r
	}
	if samples == 0 {
		return 0.0, 0
	}
	volatility = total / float64(samples)
	return
}

// Metadata is the schema for the metadata.json file.
type Metadata struct {
	Start      Date `json:"start"` // the earliest available price date
	End        Date `json:"end"`   // the latest available price date
	NumTickers int  `json:"num_tickers"`
	NumPrices  int  `json:"num_prices"`  // daily price samples
	NumMonthly int  `json:"num_monthly"` // monthly price samples
}

func (m *Metadata) UpdateTickers(tickers map[string]TickerRow) {
	m.NumTickers = len(tickers)
}

func (m *Metadata) UpdatePrices(prices []PriceRow) {
	m.NumPrices += len(prices)
	for _, p := range prices {
		if m.Start.IsZero() || m.Start.After(p.Date) {
			m.Start = p.Date
		}
		if m.End.IsZero() || m.End.Before(p.Date) {
			m.End = p.Date
		}
	}
}

func (m *Metadata) UpdateMonthly(monthly map[string][]ResampledRow) {
	m.NumMonthly = 0
	for _, ms := range monthly {
		m.NumMonthly += len(ms)
	}
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
