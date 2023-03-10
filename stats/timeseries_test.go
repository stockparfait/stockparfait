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

		ts := NewTimeseries(dates(), data())

		Convey("Init initializes correctly", func() {
			So(ts.Dates(), ShouldResemble, dates())
			So(ts.Data(), ShouldResemble, data())
			So(ts.Check(), ShouldBeNil)
		})

		Convey("Copy actually makes a copy", func() {
			dates2 := dates()
			data2 := data()
			ts := NewTimeseries(dates2, data2).Copy()
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
			dts := ts.LogProfits(1, false)
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
			ts = NewTimeseries(dates(), []float64{1.0, 0.0, 2.0, 4.0, 5.0})
			dts := ts.LogProfits(1, false)
			So(testutil.RoundSlice(dts.Data(), 5), ShouldResemble,
				testutil.RoundSlice([]float64{
					math.Inf(-1),
					math.Inf(1),
					math.Log(4.0 / 2.0),
					math.Log(5.0 / 4.0),
				}, 5))
		})

		Convey("LogProfits on too short Timeseries", func() {
			lp := ts.LogProfits(len(data())+1, false)
			So(len(lp.Data()), ShouldEqual, 0)
		})

		Convey("LogProfits on intraday data", func() {
			dates := []db.Date{
				db.NewDatetime(2020, 1, 1, 9, 30, 0, 0),
				db.NewDatetime(2020, 1, 1, 10, 30, 0, 0),
				db.NewDatetime(2020, 1, 2, 9, 30, 0, 0),
				db.NewDatetime(2020, 1, 2, 11, 30, 0, 0),
			}
			data := []float64{1, math.Exp(0.1), math.Exp(0.2), math.Exp(0.3)}
			ts := NewTimeseries(dates, data).LogProfits(1, true)
			So(ts.Dates(), ShouldResemble, []db.Date{dates[1], dates[3]})
			So(testutil.RoundSlice(ts.Data(), 5), ShouldResemble, []float64{0.1, 0.1})
		})

		Convey("FromPrices", func() {
			dt1 := db.NewDate(2000, 1, 1)
			dt2 := db.NewDate(2005, 1, 1)
			// split factor = 2, full adj. factor = 3
			prices := []db.PriceRow{
				db.TestPriceRow(dt1, 12, 6, 4, 5, 6, 3, 1000.0, true),
				db.TestPriceRow(dt2, 15, 7.5, 5, 4, 7, 3, 2000.0, true),
			}

			Convey("Open Unadjusted", func() {
				ts := NewTimeseriesFromPrices(prices, PriceOpenUnadjusted)
				So(ts.Dates(), ShouldResemble, []db.Date{dt1, dt2})
				So(ts.Data(), ShouldResemble, []float64{5.0 * 3, 4.0 * 3})
			})

			Convey("Open Split Adjusted", func() {
				ts := NewTimeseriesFromPrices(prices, PriceOpenSplitAdjusted)
				So(ts.Dates(), ShouldResemble, []db.Date{dt1, dt2})
				So(ts.Data(), ShouldResemble, []float64{5.0 * 3 / 2, 4.0 * 3 / 2})
			})

			Convey("Open Fully Adjusted", func() {
				ts := NewTimeseriesFromPrices(prices, PriceOpenFullyAdjusted)
				So(ts.Dates(), ShouldResemble, []db.Date{dt1, dt2})
				So(ts.Data(), ShouldResemble, []float64{5.0, 4.0})
			})

			Convey("High Unadjusted", func() {
				ts := NewTimeseriesFromPrices(prices, PriceHighUnadjusted)
				So(ts.Dates(), ShouldResemble, []db.Date{dt1, dt2})
				So(ts.Data(), ShouldResemble, []float64{6.0 * 3, 7.0 * 3})
			})

			Convey("High Split Adjusted", func() {
				ts := NewTimeseriesFromPrices(prices, PriceHighSplitAdjusted)
				So(ts.Dates(), ShouldResemble, []db.Date{dt1, dt2})
				So(ts.Data(), ShouldResemble, []float64{6.0 * 3 / 2, 7.0 * 3 / 2})
			})

			Convey("High Fully Adjusted", func() {
				ts := NewTimeseriesFromPrices(prices, PriceHighFullyAdjusted)
				So(ts.Dates(), ShouldResemble, []db.Date{dt1, dt2})
				So(ts.Data(), ShouldResemble, []float64{6.0, 7.0})
			})

			Convey("Low Unadjusted", func() {
				ts := NewTimeseriesFromPrices(prices, PriceLowUnadjusted)
				So(ts.Dates(), ShouldResemble, []db.Date{dt1, dt2})
				So(ts.Data(), ShouldResemble, []float64{3.0 * 3, 3.0 * 3})
			})

			Convey("Low Split Adjusted", func() {
				ts := NewTimeseriesFromPrices(prices, PriceLowSplitAdjusted)
				So(ts.Dates(), ShouldResemble, []db.Date{dt1, dt2})
				So(ts.Data(), ShouldResemble, []float64{3.0 * 3 / 2, 3.0 * 3 / 2})
			})

			Convey("Low Fully Adjusted", func() {
				ts := NewTimeseriesFromPrices(prices, PriceLowFullyAdjusted)
				So(ts.Dates(), ShouldResemble, []db.Date{dt1, dt2})
				So(ts.Data(), ShouldResemble, []float64{3.0, 3.0})
			})

			Convey("Close Unadjusted", func() {
				ts := NewTimeseriesFromPrices(prices, PriceCloseUnadjusted)
				So(ts.Dates(), ShouldResemble, []db.Date{dt1, dt2})
				So(ts.Data(), ShouldResemble, []float64{12, 15})
			})

			Convey("Close Split Adjusted", func() {
				ts := NewTimeseriesFromPrices(prices, PriceCloseSplitAdjusted)
				So(ts.Dates(), ShouldResemble, []db.Date{dt1, dt2})
				So(ts.Data(), ShouldResemble, []float64{6, 7.5})
			})

			Convey("Close Fully Adjusted", func() {
				ts := NewTimeseriesFromPrices(prices, PriceCloseFullyAdjusted)
				So(ts.Dates(), ShouldResemble, []db.Date{dt1, dt2})
				So(ts.Data(), ShouldResemble, []float64{4, 5})
			})

			Convey("Cash Volume", func() {
				ts := NewTimeseriesFromPrices(prices, PriceCashVolume)
				So(ts.Dates(), ShouldResemble, []db.Date{dt1, dt2})
				So(ts.Data(), ShouldResemble, []float64{1000.0, 2000.0})
			})
		})

		Convey("TimeseriesIntersect", func() {
			Convey("Second sequence ends before first", func() {
				t1 := NewTimeseries([]db.Date{
					d("2020-01-01"), d("2020-01-03"), d("2020-01-04"), d("2020-01-05"),
					d("2020-01-08")},
					[]float64{0, 1, 2, 3, 4})
				t2 := NewTimeseries([]db.Date{
					d("2020-01-02"), d("2020-01-03"), d("2020-01-05"), d("2020-01-06")},
					[]float64{5, 6, 7, 8})
				t3 := NewTimeseries([]db.Date{
					d("2020-01-03"), d("2020-01-05"), d("2020-01-06")},
					[]float64{9, 10, 11})
				dates := []db.Date{d("2020-01-03"), d("2020-01-05")}
				So(TimeseriesIntersect(t1, t2, t3), ShouldResemble, []*Timeseries{
					NewTimeseries(dates, []float64{1, 3}),
					NewTimeseries(dates, []float64{6, 7}),
					NewTimeseries(dates, []float64{9, 10}),
				})
			})

			Convey("Second sequence is shorter than first", func() {
				t1 := NewTimeseries([]db.Date{
					d("2020-01-01"), d("2020-01-03"), d("2020-01-04"), d("2020-01-05")},
					[]float64{0, 1, 2, 3})
				t2 := NewTimeseries([]db.Date{
					d("2020-01-02"), d("2020-01-03"), d("2020-01-05")},
					[]float64{5, 6, 7})
				t3 := NewTimeseries([]db.Date{
					d("2020-01-03"), d("2020-01-05"), d("2020-01-06")},
					[]float64{9, 10, 11})
				dates := []db.Date{d("2020-01-03"), d("2020-01-05")}
				So(TimeseriesIntersect(t1, t2, t3), ShouldResemble, []*Timeseries{
					NewTimeseries(dates, []float64{1, 3}),
					NewTimeseries(dates, []float64{6, 7}),
					NewTimeseries(dates, []float64{9, 10}),
				})
			})

			Convey("Empty intersection", func() {
				t1 := NewTimeseries([]db.Date{
					d("2020-01-01"), d("2020-01-03"), d("2020-01-05")},
					[]float64{0, 1, 2})
				t2 := NewTimeseries([]db.Date{d("2020-01-02"), d("2020-01-04")},
					[]float64{5, 6})
				So(TimeseriesIntersect(t1, t2), ShouldResemble, []*Timeseries{
					NewTimeseries(nil, nil),
					NewTimeseries(nil, nil),
				})
			})

			Convey("No timeseries", func() {
				So(len(TimeseriesIntersectIndices()), ShouldEqual, 0)
				So(len(TimeseriesIntersect()), ShouldEqual, 0)
			})

			Convey("Single timeseries", func() {
				dates := []db.Date{d("2020-01-02"), d("2020-01-04")}
				t := NewTimeseries(dates, []float64{1, 2})
				So(TimeseriesIntersect(t), ShouldResemble, []*Timeseries{t})
			})
		})

		Convey("Pointwise operations", func() {
			Convey("Add", func() {
				res := ts.Add(ts)
				So(res.Dates(), ShouldResemble, dates())
				So(res.Data(), ShouldResemble, []float64{2.0, 4.0, 6.0, 8.0, 10.0})
			})

			Convey("Sub", func() {
				res := ts.Sub(ts)
				So(res.Dates(), ShouldResemble, dates())
				So(res.Data(), ShouldResemble, []float64{0.0, 0.0, 0.0, 0.0, 0.0})
			})

			Convey("Mult", func() {
				res := ts.Mult(ts)
				So(res.Dates(), ShouldResemble, dates())
				So(res.Data(), ShouldResemble, []float64{1.0, 4.0, 9.0, 16.0, 25.0})
			})

			Convey("Div", func() {
				res := ts.Div(ts)
				So(res.Dates(), ShouldResemble, dates())
				So(res.Data(), ShouldResemble, []float64{1.0, 1.0, 1.0, 1.0, 1.0})
			})

			Convey("AddC", func() {
				res := ts.AddC(1.0)
				So(res.Dates(), ShouldResemble, dates())
				So(res.Data(), ShouldResemble, []float64{2.0, 3.0, 4.0, 5.0, 6.0})
			})

			Convey("SubC", func() {
				res := ts.SubC(1.0)
				So(res.Dates(), ShouldResemble, dates())
				So(res.Data(), ShouldResemble, []float64{0.0, 1.0, 2.0, 3.0, 4.0})
			})

			Convey("MultC", func() {
				res := ts.MultC(2.0)
				So(res.Dates(), ShouldResemble, dates())
				So(res.Data(), ShouldResemble, []float64{2.0, 4.0, 6.0, 8.0, 10.0})
			})

			Convey("DivC", func() {
				res := ts.DivC(2.0)
				So(res.Dates(), ShouldResemble, dates())
				So(res.Data(), ShouldResemble, []float64{0.5, 1.0, 1.5, 2.0, 2.5})
			})

			Convey("Log", func() {
				res := ts.Log()
				So(res.Dates(), ShouldResemble, dates())
				So(res.Data(), ShouldResemble, []float64{
					math.Log(1.0), math.Log(2.0), math.Log(3.0),
					math.Log(4.0), math.Log(5.0)})
			})

			Convey("Exp", func() {
				res := ts.Exp()
				So(res.Dates(), ShouldResemble, dates())
				So(res.Data(), ShouldResemble, []float64{
					math.Exp(1.0), math.Exp(2.0), math.Exp(3.0),
					math.Exp(4.0), math.Exp(5.0)})
			})
		})

		Convey("Filter", func() {
			t2 := ts.Filter(func(i int) bool { return ts.Data()[i] < 3 })
			So(t2, ShouldResemble, NewTimeseries(
				[]db.Date{d("2021-01-01"), d("2021-01-02")},
				[]float64{1, 2}))
		})
	})
}
