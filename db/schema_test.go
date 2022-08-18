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
	"testing"
	"time"
	"unsafe"

	"github.com/stockparfait/stockparfait/message"
	"github.com/stockparfait/testutil"

	. "github.com/smartystreets/goconvey/convey"
)

type TestDateMessage struct {
	Value Date
	Ptr   *Date
	Slice []Date
}

var _ message.Message = &TestDateMessage{}

func (m *TestDateMessage) InitMessage(js interface{}) error {
	return message.Init(m, js)
}

func TestSchema(t *testing.T) {
	t.Parallel()

	Convey("Lexicographic ordering works correctly", t, func() {
		Convey("Shorter list is smaller", func() {
			So(lessLex([]int{1, 2}, []int{1, 2, 0}), ShouldBeTrue)
			So(lessLex([]int{1, 2, 0}, []int{1, 2}), ShouldBeFalse)
		})
		Convey("Equal lists compare as false", func() {
			So(lessLex([]int{1, 2}, []int{1, 2}), ShouldBeFalse)
		})
		Convey("Middle element is less", func() {
			So(lessLex([]int{1, 2, 3}, []int{1, 3, 2}), ShouldBeTrue)
		})
		Convey("Middle element is greater", func() {
			So(lessLex([]int{1, 3, 2}, []int{1, 2, 3}), ShouldBeFalse)
		})
	})

	Convey("Date type", t, func() {
		Convey("creates New York's date", func() {
			// 2am UTC is the previous day in NY
			now := time.Date(2009, time.November, 10, 2, 0, 0, 0, time.UTC)
			d := DateInNY(now)
			So(d.String(), ShouldEqual, "2009-11-09")
		})

		Convey("converts to and from time correctly", func() {
			d := NewDate(2019, 1, 2)
			t := d.ToTime()
			So(t.Year(), ShouldEqual, d.Year())
			So(t.Month(), ShouldEqual, d.Month())
			So(t.Day(), ShouldEqual, d.Day())
			So(NewDateFromTime(t), ShouldResemble, d)
		})

		Convey("converts to string correctly", func() {
			d := NewDate(2019, 1, 2)
			So(d.String(), ShouldEqual, "2019-01-02")
		})

		Convey("compares the dates correctly", func() {
			So(NewDate(2019, 10, 15).After(NewDate(2018, 11, 25)), ShouldBeTrue)
			So(NewDate(2019, 10, 15).Before(NewDate(2019, 11, 25)), ShouldBeTrue)
			So(NewDate(2019, 10, 15).Before(NewDate(2019, 10, 25)), ShouldBeTrue)
			So(NewDate(2019, 10, 15).After(NewDate(2019, 10, 5)), ShouldBeTrue)
		})

		Convey("MinDate, MaxDate work correctly", func() {
			So(MinDate().IsZero(), ShouldBeTrue)
			So(MaxDate().IsZero(), ShouldBeTrue)
			d1 := NewDate(2018, 10, 15)
			d2 := NewDate(2019, 12, 1)
			d3 := NewDate(2019, 11, 30)
			// Zero value in the list shouldn't affect the result.
			So(MinDate(d1, d2, Date{}, d3), ShouldResemble, d1)
			So(MaxDate(d1, d2, Date{}, d3), ShouldResemble, d2)
		})

		Convey("DaysInMonth", func() {
			So(Date{}.DaysInMonth(), ShouldEqual, 0)
			So(NewDate(2021, 1, 1).DaysInMonth(), ShouldEqual, 31)
			So(NewDate(2021, 4, 1).DaysInMonth(), ShouldEqual, 30)
			So(NewDate(2021, 2, 1).DaysInMonth(), ShouldEqual, 28)
			So(NewDate(2020, 2, 1).DaysInMonth(), ShouldEqual, 29)
			So(NewDate(2000, 2, 1).DaysInMonth(), ShouldEqual, 29)
			So(NewDate(1900, 2, 1).DaysInMonth(), ShouldEqual, 28)
		})

		Convey("YearsTill", func() {
			So(Date{}.YearsTill(Date{}), ShouldEqual, 0.0)
			So(NewDate(2020, 4, 15).YearsTill(NewDate(2021, 4, 15)), ShouldEqual, 1.0)
			So(NewDate(2020, 1, 10).YearsTill(NewDate(2020, 7, 10)), ShouldEqual, 0.5)
			So(NewDate(2020, 4, 1).YearsTill(NewDate(2020, 4, 16)), ShouldEqual, 1.0/24.0)
		})

		Convey("InRange", func() {
			So(Date{}.InRange(Date{}, Date{}), ShouldBeFalse)
			d := NewDate(2010, 4, 1)
			So(d.InRange(Date{}, Date{}), ShouldBeTrue)
			So(d.InRange(d, NewDate(2011, 1, 1)), ShouldBeTrue)
			So(d.InRange(Date{}, d), ShouldBeTrue)
			So(d.InRange(d, Date{}), ShouldBeTrue)
			So(d.InRange(NewDate(2010, 4, 2), Date{}), ShouldBeFalse)
			So(d.InRange(Date{}, NewDate(2010, 3, 31)), ShouldBeFalse)
		})

		Convey("Monday works correctly", func() {
			// Jan 2, 2019 is Wednesday.
			So(NewDate(2019, 1, 2).Monday(), ShouldResemble, NewDate(2018, 12, 31))
		})

		Convey("MonthStart works correctly", func() {
			So(NewDate(2018, 2, 14).MonthStart(), ShouldResemble, NewDate(2018, 2, 1))
		})

		Convey("QuarterStart works correctly", func() {
			So(NewDate(2018, 2, 14).QuarterStart(), ShouldResemble, NewDate(2018, 1, 1))
			So(NewDate(2018, 5, 14).QuarterStart(), ShouldResemble, NewDate(2018, 4, 1))
			So(NewDate(2018, 8, 14).QuarterStart(), ShouldResemble, NewDate(2018, 7, 1))
			So(NewDate(2018, 11, 14).QuarterStart(), ShouldResemble, NewDate(2018, 10, 1))
		})
	})

	Convey("TickerRow", t, func() {
		Convey("TickerRowHeader works", func() {
			So(len(TickerRowHeader()), ShouldEqual, 10)
		})

		Convey("CSV works", func() {
			t := TickerRow{
				Source:      "source",
				Exchange:    "exch",
				Name:        "My name is",
				Category:    "cat",
				Sector:      "sec",
				Industry:    "ind",
				Location:    "there",
				SECFilings:  "click",
				CompanySite: "http:",
				Active:      true,
			}
			So(t.CSV(), ShouldResemble, []string{
				"source", "exch", "My name is", "cat", "sec", "ind",
				"there", "click", "http:", "TRUE",
			})
		})
	})

	Convey("ActionRow", t, func() {
		Convey("has correct size", func() {
			So(unsafe.Sizeof(ActionRow{}), ShouldEqual, 16)
		})

		Convey("TestAction works", func() {
			d := NewDate(2019, 1, 2)
			So(TestAction(d, 0.98, 0.5, true), ShouldResemble,
				ActionRow{
					Date:           d,
					DividendFactor: 0.98,
					SplitFactor:    0.5,
					Active:         true,
				})
		})

		Convey("ActionRowHeader works", func() {
			So(len(ActionRowHeader()), ShouldEqual, 4)
		})

		Convey("CSV works", func() {
			a := TestAction(NewDate(2020, 4, 15), 1.01, 2.0, false)
			So(a.CSV(), ShouldResemble, []string{"2020-04-15", "1.01", "2", "FALSE"})
		})
	})

	Convey("PriceRow", t, func() {
		Convey("has correct size", func() {
			So(unsafe.Sizeof(PriceRow{}), ShouldEqual, 20)
		})

		Convey("TestPrice works", func() {
			d := NewDate(2019, 1, 2)
			p := TestPrice(d, 100.0, 50.0, 1000.0, false)
			So(p.Date, ShouldResemble, d)
			So(p.CloseUnadjusted(), ShouldEqual, 100.0)
			So(p.CloseSplitAdjusted, ShouldEqual, 50.0)
			So(p.CloseFullyAdjusted, ShouldEqual, 50.0)
			So(p.CashVolume, ShouldEqual, 1000.0)
			So(p.Active(), ShouldBeFalse)
		})

		Convey("PriceRowHeader works", func() {
			So(len(PriceRowHeader()), ShouldEqual, 6)
		})

		Convey("CSV works", func() {
			p := TestPrice(NewDate(2019, 1, 2), 100.0, 50.0, 1000.0, false)
			So(p.CSV(), ShouldResemble, []string{
				"2019-01-02", "100", "50", "50", "1000", "FALSE"})
		})
	})

	Convey("ResampledRow", t, func() {
		Convey("has correct size", func() {
			So(unsafe.Sizeof(ResampledRow{}), ShouldEqual, 44)
		})

		Convey("TestResampled works", func() {
			do := NewDate(2019, 1, 1)
			dc := NewDate(2019, 4, 1)
			r := TestResampled(do, dc, 10.0, 11.0, 5.0, 1000.0, true)
			So(r.Close, ShouldEqual, 11.0)
		})

		Convey("ResampledRowHeader", func() {
			So(len(ResampledRowHeader()), ShouldEqual, 12)
		})

		Convey("CSV", func() {
			r := TestResampled(NewDate(2019, 1, 1), NewDate(2019, 4, 1),
				10.0, 11.0, 5.0, 1000.0, true)
			So(r.CSV(), ShouldResemble, []string{
				"10", "10", "10",
				"11", "5", "5", "1000",
				"2019-01-01", "2019-04-01",
				"0.2", "20", "TRUE",
			})
		})

		Convey("DailyVolatility", func() {
			Convey("regular case", func() {
				rows := []ResampledRow{
					TestResampled(NewDate(2020, 1, 1), NewDate(2020, 1, 31),
						100.0, 110.0, 110.0, 1000.0, true),
					TestResampled(NewDate(2020, 2, 1), NewDate(2020, 2, 29),
						112.0, 120.0, 120.0, 1000.0, true),
					TestResampled(NewDate(2020, 3, 1), NewDate(2020, 3, 31),
						118.8, 130.0, 130.0, 1000.0, true),
				}
				v, samples := DailyVolatility(rows)
				So(samples, ShouldEqual, 59)
				So(testutil.RoundFixed(v, 2), ShouldEqual, 0.01)
			})

			Convey("empty rows", func() {
				v, samples := DailyVolatility(nil)
				So(samples, ShouldEqual, 0)
				So(v, ShouldEqual, 0.0)
			})
		})
	})

	Convey("Time methods work", t, func() {
		Convey("marshals to JSON correctly", func() {
			t := NewTime(2019, 1, 5, 13, 30, 45)
			j, err := t.MarshalJSON()
			So(err, ShouldBeNil)
			So(string(j), ShouldEqual, `"2019-01-05 13:30:45"`)
		})

		Convey("unmarshals from JSON correctly", func() {
			var t Time
			So(t.UnmarshalJSON([]byte(`"2019-01-02 03:04:05"`)), ShouldBeNil)
			So(t.String(), ShouldEqual, "2019-01-02 03:04:05")

			So(t.UnmarshalJSON([]byte(`"2019-01-02"`)), ShouldBeNil)
			So(t.String(), ShouldEqual, "2019-01-02 00:00:00")
		})

		Convey("Message with Date works correctly", func() {
			Convey("with regular values", func() {
				var m TestDateMessage
				So(m.InitMessage(testutil.JSON(`
{
  "Value": "2020-02-02",
  "Ptr": "2021-03-05",
  "Slice": ["2019-01-01", "2022-05-04"]
}`)), ShouldBeNil)
				ptr := NewDate(2021, 3, 5)
				So(m, ShouldResemble, TestDateMessage{
					Value: NewDate(2020, 2, 2),
					Ptr:   &ptr,
					Slice: []Date{NewDate(2019, 1, 1), NewDate(2022, 5, 4)},
				})
			})

			Convey("with zero values", func() {
				var m TestDateMessage
				So(m.InitMessage(testutil.JSON(`{}`)), ShouldBeNil)
				So(m.Value.IsZero(), ShouldBeTrue)
				So(m.Ptr, ShouldBeNil)
				So(m.Slice, ShouldBeNil)
			})
		})
	})
}
