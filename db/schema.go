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
	"fmt"
	"time"

	"go.chromium.org/luci/common/clock"
	"go.chromium.org/luci/common/errors"
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
func NewDate(year uint16, month, day uint8) *Date {
	return &Date{year, month, day}
}

// SetFields assigns self to the given date.
func (d *Date) SetFields(year uint16, month, day uint8) {
	d.YearVal = year
	d.MonthVal = month
	d.DayVal = day
}

func (d *Date) Year() uint16 { return d.YearVal }
func (d *Date) Month() uint8 { return d.MonthVal }
func (d *Date) Day() uint8   { return d.DayVal }

// DateToday returns today's date in New York timezone.
func DateToday(ctx context.Context) (*Date, error) {
	tz := "America/New_York"
	location, err := time.LoadLocation(tz)
	if err != nil {
		return nil, errors.Annotate(err, "DateToday: failed to load timezone %s", tz).Err()
	}
	now := clock.Now(ctx).In(location)
	return NewDate(uint16(now.Year()), uint8(now.Month()), uint8(now.Day())), nil
}

// ToDatetime converts to Datetime value at midnight.
func (d *Date) ToDatetime() *Datetime {
	return NewDatetime(d.Year(), d.Month(), d.Day(), 0, 0, 0, 0)
}

// String representation of the value.
func (d *Date) String() string {
	return fmt.Sprintf("%04d-%02d-%02d", d.Year(), d.Month(), d.Day())
}

// ToTime converts Date to Time in UTC.
func (d *Date) ToTime() time.Time {
	return time.Date(int(d.Year()), time.Month(d.Month()), int(d.Day()), 0, 0, 0, 0, time.UTC)
}

// FromTime sets Date to Time in UTC.
func (d *Date) FromTime(t time.Time) {
	d.SetFields(uint16(t.Year()), uint8(t.Month()), uint8(t.Day()))
}

// Monday return a new Date of the Monday of the current date's week.
func (d *Date) Monday() *Date {
	t := d.ToTime()
	t = t.AddDate(0, 0, 1-int(t.Weekday()))
	var m Date
	m.FromTime(t)
	return &m
}

// MonthStart returns the 1st of the month of the current date.
func (d *Date) MonthStart() *Date {
	return NewDate(d.Year(), d.Month(), 1)
}

// QuarterStart returns the first day of the quarter of the current date.
func (d *Date) QuarterStart() *Date {
	return NewDate(d.Year(), (d.Month()-1)/3*3+1, 1)
}

// Before compares two Date objects for strict inequality (self < d2).
func (d *Date) Before(d2 *Date) bool {
	return lessLex([]int{int(d.Year()), int(d.Month()), int(d.Day())},
		[]int{int(d2.Year()), int(d2.Month()), int(d2.Day())})
}

// After compares two Date objects for strict inequality, self > d2.
func (d *Date) After(d2 *Date) bool {
	return d2.Before(d)
}

// MaxDate returns the latest date from the list, or nil if the list is empty.
func MaxDate(dates ...*Date) *Date {
	var max *Date
	for _, d := range dates {
		if max == nil || max.Before(d) {
			max = d
		}
	}
	return max
}

// Datetime records date and time as '2006-01-02 15:04:05.sss'. No timezone is
// assumed. The representation is chosen to fit the struct into 8 bytes.
type Datetime struct {
	YearVal  uint16
	MonthVal uint8
	DayVal   uint8
	Msec     uint32 // time of the day in milliseconds
}

// NewDatetime is the constructor for Datetime object.
func NewDatetime(year uint16, month, day, hour, minute, second uint8, msec uint32) *Datetime {
	var d Datetime
	d.SetFields(year, month, day, hour, minute, second, msec)
	return &d
}

// SetFields assigns self to the given date and time.
func (d *Datetime) SetFields(year uint16, month, day, hour, minute, second uint8, msec uint32) {
	d.YearVal = year
	d.MonthVal = month
	d.DayVal = day
	d.Msec = (uint32(hour)*3600+60*uint32(minute)+uint32(second))*1000 + uint32(msec)
}

func (d *Datetime) Year() uint16        { return d.YearVal }
func (d *Datetime) Month() uint8        { return d.MonthVal }
func (d *Datetime) Day() uint8          { return d.DayVal }
func (d *Datetime) Hour() uint8         { return uint8(d.Msec / 3600000) }
func (d *Datetime) Minute() uint8       { return uint8((d.Msec % 3600000) / 60000) }
func (d *Datetime) Second() uint8       { return uint8((d.Msec % 60000) / 1000) }
func (d *Datetime) Millisecond() uint32 { return d.Msec % 1000 }

// YearsTill computes the number of years between d and d2.
func (d *Datetime) YearsTill(d2 *Datetime) float64 {
	year := time.Duration(3600 * (24*365 + 6) * 1000_000_000)
	dt := d2.ToTime().Sub(d.ToTime())
	return float64(dt) / float64(year)
}

// String representation of the value.
func (d *Datetime) String() string {
	return fmt.Sprintf("%04d-%02d-%02dT%02d:%02d:%02d.%03d", d.Year(), d.Month(),
		d.Day(), d.Hour(), d.Minute(), d.Second(), d.Millisecond())
}

// Before compares two Datetime objects for strict inequality (self < d2).
func (d *Datetime) Before(d2 *Datetime) bool {
	return lessLex([]int{int(d.Year()), int(d.Month()), int(d.Day()), int(d.Msec)},
		[]int{int(d2.Year()), int(d2.Month()), int(d2.Day()), int(d2.Msec)})
}

// After compares two Datetime objects for strict inequality (self > d2).
func (d *Datetime) After(d2 *Datetime) bool {
	return d2.Before(d)
}

// ToTime converts Datetime to Time in UTC.
func (d *Datetime) ToTime() time.Time {
	return time.Date(int(d.Year()), time.Month(d.Month()), int(d.Day()),
		int(d.Hour()), int(d.Minute()), int(d.Second()),
		int(d.Millisecond()*1000000), time.UTC)
}
