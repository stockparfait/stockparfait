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
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestDB(t *testing.T) {
	t.Parallel()
	tmpdir, tmpdirErr := ioutil.TempDir("", "testdb")
	defer os.RemoveAll(tmpdir)

	Convey("Test setup succeeded", t, func() {
		So(tmpdirErr, ShouldBeNil)
	})

	Convey("Data access methods", t, func() {
		dbPath := filepath.Join(tmpdir, "db")
		tickers := map[string]TickerRow{
			"A": {},
			"B": {},
		}
		actions := map[string][]ActionRow{
			"A": {
				TestAction(NewDate(2019, 1, 1), 1.0, 1.0, true),
			},
			"B": {
				TestAction(NewDate(2019, 1, 1), 1.0, 1.0, true),
				TestAction(NewDate(2020, 1, 1), 1.0, 1.0, false),
			},
		}
		pricesA := []PriceRow{
			TestPrice(NewDate(2019, 1, 1), 10.0, 1000.0),
			TestPrice(NewDate(2019, 1, 2), 11.0, 1100.0),
			TestPrice(NewDate(2019, 1, 3), 12.0, 1200.0),
		}
		pricesB := []PriceRow{
			TestPrice(NewDate(2019, 1, 1), 100.0, 100.0),
			TestPrice(NewDate(2019, 1, 2), 110.0, 110.0),
			TestPrice(NewDate(2019, 1, 3), 120.0, 120.0),
		}
		monthly := map[string][]ResampledRow{
			"A": {
				TestResampled(NewDate(2019, 1, 1), NewDate(2019, 1, 31), 10.0, 12.0, 9.0, 10.0, 1000.0, true),
				TestResampled(NewDate(2019, 2, 1), NewDate(2019, 2, 28), 10.0, 12.0, 9.0, 10.0, 1000.0, true),
			},
			"B": {
				TestResampled(NewDate(2019, 1, 1), NewDate(2019, 1, 31), 100.0, 120.0, 90.0, 100.0, 1000.0, true),
				TestResampled(NewDate(2019, 2, 1), NewDate(2019, 2, 28), 100.0, 120.0, 90.0, 100.0, 1000.0, true),
			},
		}

		Convey("write methods work", func() {
			db := NewDatabase(dbPath)
			So(db.WriteTickers(tickers), ShouldBeNil)
			So(db.WriteActions(actions), ShouldBeNil)
			So(db.WritePrices("A", pricesA), ShouldBeNil)
			So(db.WritePrices("B", pricesB), ShouldBeNil)
			So(db.WriteMonthly(monthly), ShouldBeNil)
		})

		Convey("ticker access methods work", func() {
			db := NewDatabase(dbPath)
			r, err := db.TickerRow("A")
			So(err, ShouldBeNil)
			So(r, ShouldResemble, tickers["A"])

			r, err = db.TickerRow("B")
			So(err, ShouldBeNil)
			So(r, ShouldResemble, tickers["B"])

			r, err = db.TickerRow("UNKNOWN")
			So(err, ShouldNotBeNil)

			ts, err := db.Tickers(NewConstraints().Ticker("A", "OTHER"))
			So(err, ShouldBeNil)
			So(ts, ShouldResemble, []string{"A"})
		})

		Convey("action access methods work", func() {
			db := NewDatabase(dbPath)
			a, err := db.Actions("A", NewConstraints())
			So(err, ShouldBeNil)
			So(a, ShouldResemble, actions["A"])

			a, err = db.Actions("B", NewConstraints().EndAt(NewDate(2019, 6, 1)))
			So(err, ShouldBeNil)
			So(a, ShouldResemble, actions["B"][:1])
		})

		Convey("price access methods work", func() {
			db := NewDatabase(dbPath)
			p, err := db.Prices("A", NewConstraints())
			So(err, ShouldBeNil)
			So(p, ShouldResemble, pricesA)

			p, err = db.Prices("B", NewConstraints().StartAt(NewDate(2019, 1, 2)))
			So(err, ShouldBeNil)
			So(p, ShouldResemble, pricesB[1:])
		})

		Convey("monthly access methods work", func() {
			db := NewDatabase(dbPath)
			a, err := db.Monthly("A", NewConstraints())
			So(err, ShouldBeNil)
			So(a, ShouldResemble, monthly["A"])

			a, err = db.Monthly("B", NewConstraints().EndAt(NewDate(2019, 2, 15)))
			So(err, ShouldBeNil)
			So(a, ShouldResemble, monthly["B"][:1])
		})
	})
}
