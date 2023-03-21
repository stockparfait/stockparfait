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
	"math"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stockparfait/testutil"

	. "github.com/smartystreets/goconvey/convey"
)

func TestRanges(t *testing.T) {
	t.Parallel()

	Convey("Interval works", t, func() {
		Convey("from config", func() {
			var i Interval
			So(i.InitMessage(testutil.JSON(`
{
  "min": -1.5,
  "max": 100,
  "start": "2020-02-02",
  "end": "2021-04-21"
}`)), ShouldBeNil)
			min := -1.5
			max := 100.0
			So(i, ShouldResemble, Interval{
				Min:   &min,
				Max:   &max,
				Start: NewDate(2020, 2, 2),
				End:   NewDate(2021, 4, 21),
			})
			So(i.ValueInRange(50.0), ShouldBeTrue)
			So(i.ValueInRange(-50.0), ShouldBeFalse)
			So(i.ValueInRange(150.0), ShouldBeFalse)
			So(i.DateInRange(NewDate(2020, 5, 1)), ShouldBeTrue)
			So(i.DateInRange(NewDate(2020, 2, 1)), ShouldBeFalse)
		})

		Convey("optional bounds", func() {
			i := Interval{}
			So(i.ValueInRange(0.0), ShouldBeTrue)
			So(i.DateInRange(NewDate(2020, 1, 1)), ShouldBeTrue)
		})
	})

	Convey("IntradayRange works", t, func() {
		var r IntradayRange
		Convey("with all bounds", func() {
			So(r.InitMessage(testutil.JSON(`
{
  "start": "9:30:00.001",
  "end": "16:00"
}`)), ShouldBeNil)
			So(r.InRange(NewTimeOfDay(9, 30, 0, 1)), ShouldBeTrue)
			So(r.InRange(NewTimeOfDay(9, 29, 59, 999)), ShouldBeFalse)
			So(r.InRange(NewTimeOfDay(16, 0, 0, 1)), ShouldBeFalse)
		})

		Convey("without start", func() {
			So(r.InitMessage(testutil.JSON(`
{
  "end": "16:00"
}`)), ShouldBeNil)
			So(r.InRange(NewTimeOfDay(0, 30, 0, 1)), ShouldBeTrue)
			So(r.InRange(NewTimeOfDay(16, 0, 0, 1)), ShouldBeFalse)
		})
	})
}

func TestDB(t *testing.T) {
	t.Parallel()
	tmpdir, tmpdirErr := os.MkdirTemp("", "testdb")
	defer os.RemoveAll(tmpdir)

	Convey("Test setup succeeded", t, func() {
		So(tmpdirErr, ShouldBeNil)
	})

	Convey("DB methods work", t, func() {
		dbName := "testdb"
		dbPath := filepath.Join(tmpdir, dbName)
		tickers := map[string]TickerRow{
			"A": {},
			"B": {},
		}
		pricesA := []PriceRow{
			TestPrice(NewDatetime(2019, 1, 1, 9, 30, 0, 0), 10.0, 10.0, 10.0, 1000.0, true),
			TestPrice(NewDatetime(2019, 1, 2, 12, 0, 0, 0), 11.0, 11.0, 11.0, 1100.0, true),
			TestPrice(NewDatetime(2019, 1, 3, 17, 9, 59, 0), 12.0, 12.0, 12.0, 1200.0, true),
		}
		pricesB := []PriceRow{
			TestPrice(NewDate(2019, 1, 1), 100.0, 100.0, 100.0, 100.0, true),
			TestPrice(NewDate(2019, 1, 2), 110.0, 110.0, 110.0, 110.0, true),
			TestPrice(NewDate(2019, 1, 3), 120.0, 120.0, 120.0, 120.0, true),
		}
		monthly := map[string][]ResampledRow{
			"A": {
				TestResampled(NewDate(2019, 1, 1), NewDate(2019, 1, 31), 10.0, 10.0, 10.0, 1000.0, true),
				TestResampled(NewDate(2019, 2, 1), NewDate(2019, 2, 28), 10.0, 10.0, 10.0, 1000.0, true),
			},
			"B": {
				TestResampled(NewDate(2019, 1, 1), NewDate(2019, 1, 31), 100.0, 150.0, 120.0, 1000.0, true),
				TestResampled(NewDate(2019, 2, 1), NewDate(2019, 2, 28), 150.0, 200.0, 200.0, 2000.0, true),
			},
		}

		Convey("write methods work", func() {
			w := NewWriter(tmpdir, dbName)
			So(w.WriteTickers(tickers), ShouldBeNil)
			So(w.WritePrices("A", pricesA), ShouldBeNil)
			So(w.WritePrices("B", pricesB), ShouldBeNil)
			So(w.WriteMonthly(monthly), ShouldBeNil)
			So(w.WriteMetadata(w.Metadata), ShouldBeNil)
		})

		Convey("ComputeMonthly works", func() {
			daily := []PriceRow{
				TestPrice(NewDate(2020, 1, 3), 100.0, 50.0, 50.0, 1000.0, true),
				TestPrice(NewDate(2020, 1, 4), 110.0, 55.0, 55.0, 1000.0, true),
				TestPrice(NewDate(2020, 1, 15), 102.0, 51.0, 51.0, 2000.0, true),
				TestPrice(NewDate(2020, 2, 5), 130.0, 65.0, 65.0, 1000.0, true),
				TestPrice(NewDate(2020, 2, 6), 140.0, 70.0, 70.0, 1000.0, true),
				TestPrice(NewDate(2020, 2, 7), 150.0, 75.0, 75.0, 1000.0, false),
				TestPrice(NewDate(2020, 3, 1), 160.0, 80.0, 80.0, 500.0, true),
			}
			So(ComputeMonthly(daily), ShouldResemble, []ResampledRow{
				{
					DateOpen:           NewDate(2020, 1, 3),
					DateClose:          NewDate(2020, 1, 15),
					Open:               100.0,
					OpenSplitAdjusted:  50.0,
					OpenFullyAdjusted:  50.0,
					Close:              102.0,
					CloseSplitAdjusted: 51.0,
					CloseFullyAdjusted: 51.0,
					CashVolume:         4000.0,
					SumAbsLogProfits:   float32(math.Log(55.0)-math.Log(50.0)) + float32(math.Log(55.0)-math.Log(51.0)),
					NumSamples:         3,
					Active:             true,
				},
				{
					DateOpen:           NewDate(2020, 2, 5),
					DateClose:          NewDate(2020, 2, 7),
					Open:               130.0,
					OpenSplitAdjusted:  65.0,
					OpenFullyAdjusted:  65.0,
					Close:              150.0,
					CloseSplitAdjusted: 75.0,
					CloseFullyAdjusted: 75.0,
					CashVolume:         3000.0,
					SumAbsLogProfits:   float32(math.Log(75.0) - math.Log(65.0)),
					NumSamples:         3,
					Active:             false,
				},
				{
					DateOpen:           NewDate(2020, 3, 1),
					DateClose:          NewDate(2020, 3, 1),
					Open:               160.0,
					OpenSplitAdjusted:  80.0,
					OpenFullyAdjusted:  80.0,
					Close:              160.0,
					CloseSplitAdjusted: 80.0,
					CloseFullyAdjusted: 80.0,
					CashVolume:         500.0,
					SumAbsLogProfits:   0.0,
					NumSamples:         1,
					Active:             true,
				},
			})
		})

		Convey("ticker access methods work", func() {
			db := NewReader(tmpdir, dbName)
			ctx := context.Background()

			r, err := db.TickerRow("A")
			So(err, ShouldBeNil)
			So(r, ShouldResemble, tickers["A"])

			r, err = db.TickerRow("B")
			So(err, ShouldBeNil)
			So(r, ShouldResemble, tickers["B"])

			_, err = db.TickerRow("UNKNOWN")
			So(err, ShouldNotBeNil)

			db.UseTickers = []string{"A", "OTHER"}

			ts, err := db.Tickers(ctx)
			So(err, ShouldBeNil)
			So(ts, ShouldResemble, []string{"A"})
		})

		Convey("filtering tickers with monthly data", func() {
			db := NewReader(tmpdir, dbName)
			ctx := context.Background()

			tickers, err := db.Tickers(ctx)
			So(err, ShouldBeNil)
			sort.Slice(tickers, func(i, j int) bool { return tickers[i] < tickers[j] })
			So(tickers, ShouldResemble, []string{"A", "B"})

			max := 1.5
			db.YearlyGrowth = &Interval{Max: &max}
			tickers, err = db.Tickers(ctx)
			So(err, ShouldBeNil)
			So(tickers, ShouldResemble, []string{"A"})

			db.YearlyGrowth = nil
			min := 60.0
			db.CashVolume = &Interval{Min: &min}
			tickers, err = db.Tickers(ctx)
			So(err, ShouldBeNil)
			So(tickers, ShouldResemble, []string{"B"})

			db.CashVolume = nil
			min = 0.02
			db.Volatility = &Interval{Min: &min}
			tickers, err = db.Tickers(ctx)
			So(err, ShouldBeNil)
			So(len(tickers), ShouldEqual, 0)
		})

		Convey("filtering intraday prices", func() {
			start := NewTimeOfDay(10, 0, 0, 0)
			end := NewTimeOfDay(16, 0, 0, 0)
			db := NewReader(tmpdir, dbName)
			db.Intraday = &IntradayRange{
				Start: &start,
				End:   &end,
			}
			p, err := db.Prices("A")
			So(err, ShouldBeNil)
			So(p, ShouldResemble, []PriceRow{pricesA[1]})
		})

		Convey("price access methods work", func() {
			db := NewReader(tmpdir, dbName)
			p, err := db.Prices("A")
			So(err, ShouldBeNil)
			So(p, ShouldResemble, pricesA)

			db.Start = NewDate(2019, 1, 2)

			p, err = db.Prices("B")
			So(err, ShouldBeNil)
			So(p, ShouldResemble, pricesB[1:])
		})

		Convey("monthly access methods work", func() {
			db := NewReader(tmpdir, dbName)
			a, err := db.Monthly("A", Date{}, Date{})
			So(err, ShouldBeNil)
			So(a, ShouldResemble, monthly["A"])

			a, err = db.Monthly("B", NewDate(2019, 1, 15), Date{})
			So(err, ShouldBeNil)
			So(a, ShouldResemble, monthly["B"][1:])

			db.End = NewDate(2019, 2, 15)

			a, err = db.Monthly("B", Date{}, Date{})
			So(err, ShouldBeNil)
			So(a, ShouldResemble, monthly["B"][:1])
		})

		Convey("metadata access methods work", func() {
			db := NewReader(tmpdir, dbName)
			m, err := db.Metadata()
			So(err, ShouldBeNil)
			So(m, ShouldResemble, Metadata{
				Start:      NewDate(2019, 1, 1),
				End:        NewDatetime(2019, 1, 3, 17, 9, 59, 0),
				NumTickers: 2,
				NumPrices:  6,
				NumMonthly: 4,
			})
		})

		Convey("Reader from config", func() {
			var r Reader
			js := fmt.Sprintf(`{
  "DB path": "%s",
  "DB": "%s",
  "sources": ["S1", "S2"],
  "tickers": ["A", "B"],
  "exclude tickers": ["B", "C"],
  "exchanges": ["E1", "E2"],
  "names": ["n1", "n2"],
  "categories": ["c1", "c2"],
  "sectors": ["s1", "s2"],
  "industries": ["i1", "i2"],
  "start": "2021-01-01",
  "end": "2021-10-02"
  }`, tmpdir, dbName)
			So(r.InitMessage(testutil.JSON(js)), ShouldBeNil)
			So(r.cachePath(), ShouldEqual, dbPath)
			r.initConstraints()
			So(r.constraints, ShouldResemble, &Constraints{
				Sources:        map[string]struct{}{"S1": {}, "S2": {}},
				Tickers:        map[string]struct{}{"A": {}, "B": {}},
				ExcludeTickers: map[string]struct{}{"B": {}, "C": {}},
				Exchanges:      map[string]struct{}{"E1": {}, "E2": {}},
				Names:          map[string]struct{}{"n1": {}, "n2": {}},
				Categories:     map[string]struct{}{"c1": {}, "c2": {}},
				Sectors:        map[string]struct{}{"s1": {}, "s2": {}},
				Industries:     map[string]struct{}{"i1": {}, "i2": {}},
			})
		})

		Convey("Cleanup works", func() {
			w := NewWriter(tmpdir, dbName)
			So(w.WritePrices("C", pricesB), ShouldBeNil) // C is not in tickers
			So(fileExists(pricesFile(w.cachePath(), "C")), ShouldBeTrue)

			ctx := context.Background()
			So(Cleanup(ctx, tmpdir, dbName), ShouldBeNil)
			So(fileExists(pricesFile(w.cachePath(), "C")), ShouldBeFalse)
			So(fileExists(pricesFile(w.cachePath(), "A")), ShouldBeTrue)
			So(fileExists(pricesFile(w.cachePath(), "B")), ShouldBeTrue)
		})
	})
}
