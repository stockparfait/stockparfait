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

		Convey("Deltas default", func() {
			dts, err := ts.Deltas(DeltaParams{})
			So(err, ShouldBeNil)
			So(dts.Dates(), ShouldResemble, dates()[1:])
			So(dts.Data(), ShouldResemble, []float64{1.0, 1.0, 1.0, 1.0})
		})

		Convey("Deltas relative", func() {
			dts, err := ts.Deltas(DeltaParams{Relative: true})
			So(err, ShouldBeNil)
			So(dts.Dates(), ShouldResemble, dates()[1:])
			So(dts.Data(), ShouldResemble, []float64{1.0, 0.5, 1.0 / 3.0, 0.25})
		})

		Convey("Deltas relative with zeros", func() {
			// Relative delta after 0.0 value should be skipped.
			ts = NewTimeseries().Init(dates(), []float64{1.0, 0.0, 2.0, 4.0, 5.0})
			dts, err := ts.Deltas(DeltaParams{Relative: true})
			So(err, ShouldBeNil)
			So(dts.Dates(), ShouldResemble, []db.Date{
				dates()[1], dates()[3], dates()[4]})
			So(dts.Data(), ShouldResemble, []float64{-1.0, 1.0, 0.25})
		})

		Convey("Deltas logarithmic", func() {
			dts, err := ts.Deltas(DeltaParams{Log: true})
			So(err, ShouldBeNil)
			So(dts.Dates(), ShouldResemble, dates()[1:])
			roundSlice := func(x []float64) []float64 {
				res := make([]float64, len(x))
				for i := range x {
					res[i] = math.Round(x[i]*1000_000.0) / 1000_000.0
				}
				return res
			}
			So(roundSlice(dts.Data()), ShouldResemble, roundSlice([]float64{
				math.Log(2.0), math.Log(3.0 / 2.0), math.Log(4.0 / 3.0),
				math.Log(5.0 / 4.0)}))
		})

		Convey("Deltas normalized", func() {
			ts = NewTimeseries().Init(dates(), []float64{1.0, 3.0, 2.0, 5.0, 4.0})
			// Raw deltas: 2, -1, 3, -1; mean = 3/4 = 0.75
			mad := ((2.0 - 0.75) + (1.0 + 0.75) + (3.0 - 0.75) + (1 + 0.75)) / 4.0
			dts, err := ts.Deltas(DeltaParams{Normalized: true})
			So(err, ShouldBeNil)
			So(dts.Dates(), ShouldResemble, dates()[1:])
			So(dts.Data(), ShouldResemble, []float64{
				(2.0 - 0.75) / mad, (-1.0 - 0.75) / mad,
				(3.0 - 0.75) / mad, (-1.0 - 0.75) / mad})
		})

		Convey("Deltas normalized with MAD=0", func() {
			_, err := ts.Deltas(DeltaParams{Normalized: true})
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "MAD(deltas)=0")
		})

		Convey("FromPrices", func() {
			dt1 := db.NewDate(2000, 1, 1)
			dt2 := db.NewDate(2005, 1, 1)
			prices := []db.PriceRow{
				db.TestPrice(dt1, 10.0, 5.0, 1000.0, true),
				db.TestPrice(dt2, 12.0, 6.0, 2000.0, true),
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

			Convey("Dollar Volume", func() {
				ts := NewTimeseries().FromPrices(prices, PriceDollarVolume)
				So(ts.Dates(), ShouldResemble, []db.Date{dt1, dt2})
				So(ts.Data(), ShouldResemble, []float64{1000.0, 2000.0})
			})
		})
	})
}
