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
	"math"
	"testing"
	"time"

	"go.chromium.org/luci/common/clock/testclock"

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
		Convey("gets today's date", func() {
			// 2am UTC is the previous day in NY
			now := time.Date(2009, time.November, 10, 2, 0, 0, 0, time.UTC)
			ctx, _ := testclock.UseTime(context.Background(), now)
			d, err := DateToday(ctx)
			So(err, ShouldBeNil)
			So(d.String(), ShouldEqual, "2009-11-09")
		})

		Convey("converts to Datetime correctly", func() {
			d := NewDate(2019, 1, 2)
			So(d.ToDatetime(), ShouldResemble, NewDatetime(2019, 1, 2, 0, 0, 0, 0))
		})

		Convey("compares the dates correctly", func() {
			So(NewDate(2019, 10, 15).After(NewDate(2018, 11, 25)), ShouldBeTrue)
			So(NewDate(2019, 10, 15).Before(NewDate(2019, 11, 25)), ShouldBeTrue)
			So(NewDate(2019, 10, 15).Before(NewDate(2019, 10, 25)), ShouldBeTrue)
			So(NewDate(2019, 10, 15).After(NewDate(2019, 10, 5)), ShouldBeTrue)
		})

		Convey("MaxDate works correctly", func() {
			So(MaxDate(), ShouldBeNil)
			d1 := NewDate(2018, 10, 15)
			d2 := NewDate(2019, 12, 1)
			d3 := NewDate(2019, 11, 30)
			So(MaxDate(d1, d2, d3), ShouldEqual, d2)
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

	Convey("Datetime type", t, func() {
		Convey("creates new value correctly", func() {
			d := NewDatetime(2019, 1, 2, 3, 4, 5, 99)
			So(d.Msec, ShouldEqual, uint32((3*3600+4*60+5)*1000+99))
		})

		Convey("correctly computes YearsTill", func() {
			d := NewDatetime(2019, 1, 1, 0, 0, 0, 0)
			d2 := NewDatetime(2021, 7, 1, 0, 0, 0, 0)
			So(math.Round(d.YearsTill(d2)*10.0)/10.0, ShouldEqual, 2.5)
		})

		Convey("compares the dates correctly", func() {
			So(NewDatetime(2019, 10, 15, 1, 2, 3, 55).Before(
				NewDatetime(2019, 10, 15, 1, 2, 3, 55)), ShouldBeFalse)
			So(NewDatetime(2019, 10, 15, 0, 0, 0, 0).After(
				NewDatetime(2018, 11, 25, 0, 0, 0, 0)), ShouldBeTrue)
			So(NewDatetime(2019, 10, 15, 1, 2, 3, 55).Before(
				NewDatetime(2019, 10, 15, 1, 2, 3, 56)), ShouldBeTrue)
		})
	})
}
