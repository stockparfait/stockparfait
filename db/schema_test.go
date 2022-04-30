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

	. "github.com/smartystreets/goconvey/convey"
)

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

		Convey("MaxDate works correctly", func() {
			So(MaxDate().IsZero(), ShouldBeTrue)
			d1 := NewDate(2018, 10, 15)
			d2 := NewDate(2019, 12, 1)
			d3 := NewDate(2019, 11, 30)
			So(MaxDate(d1, d2, d3), ShouldResemble, d2)
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

	})

	Convey("PriceRow", t, func() {
		Convey("has correct size", func() {
			So(unsafe.Sizeof(PriceRow{}), ShouldEqual, 20)
		})

		Convey("TestPrice works", func() {
			d := NewDate(2019, 1, 2)
			p := TestPrice(d, 100.0, 50.0, 1000.0)
			So(p.Date, ShouldResemble, d)
			So(p.Close, ShouldEqual, 100.0)
			So(p.CloseSplitAdjusted, ShouldEqual, 50.0)
			So(p.CloseFullyAdjusted, ShouldEqual, 50.0)
			So(p.DollarVolume, ShouldEqual, 1000.0)
		})
	})

	Convey("ResampledRow", t, func() {
		Convey("has correct size", func() {
			So(unsafe.Sizeof(ResampledRow{}), ShouldEqual, 48)
		})

		Convey("TestResampled works", func() {
			do := NewDate(2019, 1, 1)
			dc := NewDate(2019, 4, 1)
			r := TestResampled(do, dc, 10.0, 12.0, 9.0, 11.0, 1000.0, true)
			So(r.Close, ShouldEqual, 11.0)
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
	})
}
