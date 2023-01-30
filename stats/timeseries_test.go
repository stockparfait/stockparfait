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
	"testing"

	"github.com/stockparfait/stockparfait/db"
	"github.com/stockparfait/testutil"

	. "github.com/smartystreets/goconvey/convey"
)

func TestTimeseries(t *testing.T) {
	d := func(s string) db.Date {
		res, err := db.NewDateFromString(s)
		if err != nil {
			panic(err)
		}
		return res
	}

	dates := func() []db.Date {
		return []db.Date{
			d("2021-01-01"),
			d("2021-01-02"),
			d("2021-01-03"),
			d("2021-01-04"),
			d("2021-01-05"),
		}
	}
	data := func() []float64 { return []float64{1.0, 2.0, 3.0, 4.0, 5.0} }

	Convey("Timeseries methods work", t, func() {

		ts := NewTimeseries().Init(dates(), data())

		Convey("Init initializes correctly", func() {
			So(ts.Dates(), ShouldResemble, dates())
			So(ts.Data(), ShouldResemble, data())
			So(ts.Check(), ShouldBeNil)
		})

		Convey("Copy actually makes a copy", func() {
			dates2 := dates()
			data2 := data()
			ts := NewTimeseries().Copy(dates2, data2)
			dates2[3] = d("2000-10-10")
			data2[3] = 200.0
			So(ts.Dates(), ShouldResemble, dates())
			So(ts.Data(), ShouldResemble, data())
			So(ts.Check(), ShouldBeNil)
		})

		Convey("Range", func() {
			r := ts.Range(d("2021-01-02"), d("2021-01-04"))
			So(r.Dates(), ShouldResemble, dates()[1:4])
			So(r.Data(), ShouldResemble, data()[1:4])

			r = ts.Range(d("2020-12-31"), d("2021-01-06"))
			So(r, ShouldResemble, ts)

			r = ts.Range(d("2021-01-05"), d("2021-01-04"))
			So(len(r.Dates()), ShouldEqual, 0)
		})

		Convey("Shift", func() {
			r := ts.Shift(0)
			So(r, ShouldResemble, ts)

			r = ts.Shift(2)
			So(r.Dates(), ShouldResemble, dates()[2:])
			So(r.Data(), ShouldResemble, data()[:3])

			r = ts.Shift(-2)
			So(r.Dates(), ShouldResemble, dates()[:3])
			So(r.Data(), ShouldResemble, data()[2:])
		})

		Convey("LogProfits", func() {
			dts := ts.LogProfits(1)
			So(ts.Data(), ShouldResemble, data()) // the original ts is not modified
			So(dts.Dates(), ShouldResemble, ts.Dates()[1:])
			So(testutil.RoundSlice(dts.Data(), 5), ShouldResemble,
				testutil.RoundSlice([]float64{
					math.Log(2.0),
					math.Log(3.0 / 2.0),
					math.Log(4.0 / 3.0),
					math.Log(5.0 / 4.0),
				}, 5))
		})

		Convey("LogProfits with zeros", func() {
			ts = NewTimeseries().Init(dates(), []float64{1.0, 0.0, 2.0, 4.0, 5.0})
			dts := ts.LogProfits(1)
			So(testutil.RoundSlice(dts.Data(), 5), ShouldResemble,
				testutil.RoundSlice([]float64{
					math.Inf(-1),
					math.Inf(1),
					math.Log(4.0 / 2.0),
					math.Log(5.0 / 4.0),
				}, 5))
		})

		Convey("FromPrices", func() {
			dt1 := db.NewDate(2000, 1, 1)
			dt2 := db.NewDate(2005, 1, 1)
			prices := []db.PriceRow{
				db.TestPrice(dt1, 10.0, 5.0, 5.0, 1000.0, true),
				db.TestPrice(dt2, 12.0, 6.0, 6.0, 2000.0, true),
			}

			Convey("Unadjusted", func() {
				ts := NewTimeseries().FromPrices(prices, PriceUnadjusted)
				So(ts.Dates(), ShouldResemble, []db.Date{dt1, dt2})
				So(ts.Data(), ShouldResemble, []float64{10.0, 12.0})
			})

			Convey("Split Adjusted", func() {
				ts := NewTimeseries().FromPrices(prices, PriceSplitAdjusted)
				So(ts.Dates(), ShouldResemble, []db.Date{dt1, dt2})
				So(ts.Data(), ShouldResemble, []float64{5.0, 6.0})
			})

			Convey("Fully Adjusted", func() {
				ts := NewTimeseries().FromPrices(prices, PriceFullyAdjusted)
				So(ts.Dates(), ShouldResemble, []db.Date{dt1, dt2})
				So(ts.Data(), ShouldResemble, []float64{5.0, 6.0})
			})

			Convey("Cash Volume", func() {
				ts := NewTimeseries().FromPrices(prices, PriceCashVolume)
				So(ts.Dates(), ShouldResemble, []db.Date{dt1, dt2})
				So(ts.Data(), ShouldResemble, []float64{1000.0, 2000.0})
			})
		})

		Convey("TimeseriesIntersectIndices", func() {
			Convey("Second sequence ends before first", func() {
				t1 := NewTimeseries().Init([]db.Date{
					d("2020-01-01"), d("2020-01-03"), d("2020-01-04"), d("2020-01-05"),
					d("2020-01-08")},
					make([]float64, 5))
				t2 := NewTimeseries().Init([]db.Date{
					d("2020-01-02"), d("2020-01-03"), d("2020-01-05"), d("2020-01-06")},
					make([]float64, 4))
				t3 := NewTimeseries().Init([]db.Date{
					d("2020-01-03"), d("2020-01-05"), d("2020-01-06")},
					make([]float64, 3))
				So(TimeseriesIntersectIndices(t1, t2, t3), ShouldResemble, [][]int{
					{1, 1, 0},
					{3, 2, 1},
				})
			})

			Convey("Second sequence is shorter than first", func() {
				t1 := NewTimeseries().Init([]db.Date{
					d("2020-01-01"), d("2020-01-03"), d("2020-01-04"), d("2020-01-05")},
					make([]float64, 4))
				t2 := NewTimeseries().Init([]db.Date{
					d("2020-01-02"), d("2020-01-03"), d("2020-01-05")},
					make([]float64, 3))
				t3 := NewTimeseries().Init([]db.Date{
					d("2020-01-03"), d("2020-01-05"), d("2020-01-06")},
					make([]float64, 3))
				So(TimeseriesIntersectIndices(t1, t2, t3), ShouldResemble, [][]int{
					{1, 1, 0},
					{3, 2, 1},
				})
			})

			Convey("Empty intersection", func() {
				t1 := NewTimeseries().Init([]db.Date{
					d("2020-01-01"), d("2020-01-03"), d("2020-01-05")},
					make([]float64, 3))
				t2 := NewTimeseries().Init([]db.Date{d("2020-01-02"), d("2020-01-04")},
					make([]float64, 2))
				So(len(TimeseriesIntersectIndices(t1, t2)), ShouldEqual, 0)
			})

			Convey("No timeseries", func() {
				So(len(TimeseriesIntersectIndices()), ShouldEqual, 0)
			})

			Convey("Single timeseries", func() {
				t := NewTimeseries().Init([]db.Date{d("2020-01-02"), d("2020-01-04")},
					make([]float64, 2))
				So(TimeseriesIntersectIndices(t), ShouldResemble, [][]int{{0}, {1}})
			})
		})
	})
}
