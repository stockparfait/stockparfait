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

package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stockparfait/logging"
	"github.com/stockparfait/stockparfait/db"
	"github.com/stockparfait/testutil"

	. "github.com/smartystreets/goconvey/convey"
)

func TestMain(t *testing.T) {
	t.Parallel()

	tmpdir, tmpdirErr := os.MkdirTemp("", "test_screener")
	defer os.RemoveAll(tmpdir)

	Convey("Setup succeeded", t, func() {
		So(tmpdirErr, ShouldBeNil)
	})

	Convey("parseFlags", t, func() {
		flags, err := parseFlags([]string{
			"-conf", "path/to/config", "-log-level", "warning", "-csv"})
		So(err, ShouldBeNil)
		So(flags.Config, ShouldEqual, "path/to/config")
		So(flags.LogLevel, ShouldEqual, logging.Warning)
		So(flags.CSV, ShouldBeTrue)
	})

	Convey("printData works", t, func() {
		dbName := "testdb"
		tickers := map[string]db.TickerRow{
			"A": {
				Source:      "test",
				Exchange:    "ex A",
				Name:        "Name A",
				Category:    "cat A",
				Sector:      "sec A",
				Industry:    "ind A",
				Location:    "loc A",
				SECFilings:  "SEC A",
				CompanySite: "site A",
				Active:      true,
			},
			"B": {
				Source:      "test",
				Exchange:    "ex B",
				Name:        "Name B",
				Category:    "cat B",
				Sector:      "sec B",
				Industry:    "ind B",
				Location:    "loc B",
				SECFilings:  "SEC B",
				CompanySite: "site B",
				Active:      false,
			},
		}
		pricesA := []db.PriceRow{
			db.TestPrice(db.NewDate(2019, 1, 1), 10.0, 10.0, 10.0, 1000.0, true),
			db.TestPrice(db.NewDate(2019, 1, 2), 11.0, 11.0, 11.0, 1100.0, true),
			db.TestPrice(db.NewDate(2019, 1, 3), 12.0, 12.0, 12.0, 1200.0, false),
		}
		pricesB := []db.PriceRow{
			db.TestPrice(db.NewDate(2019, 1, 1), 100.0, 100.0, 100.0, 1000.0, true),
			db.TestPrice(db.NewDate(2019, 1, 2), 110.0, 110.0, 110.0, 1100.0, true),
			db.TestPrice(db.NewDate(2019, 1, 3), 120.0, 120.0, 120.0, 1200.0, false),
		}
		w := db.NewWriter(tmpdir, dbName)
		So(w.WriteTickers(tickers), ShouldBeNil)
		So(w.WritePrices("A", pricesA), ShouldBeNil)
		So(w.WritePrices("B", pricesB), ShouldBeNil)

		ctx := context.Background()
		configFile := filepath.Join(tmpdir, "config.json")

		Convey("print text", func() {
			So(testutil.WriteFile(configFile, fmt.Sprintf(`
{
  "data": {"DB path": "%s", "DB": "%s"},
  "columns": [
    {"kind": "ticker", "sort": "ascending"},
    {"kind": "price", "date": "2019-01-02"}
  ]
}`, tmpdir, dbName)),
				ShouldBeNil)
			flags, err := parseFlags([]string{"-conf", configFile})
			So(err, ShouldBeNil)
			var buf bytes.Buffer
			So(printData(ctx, flags, &buf), ShouldBeNil)
			So("\n"+buf.String(), ShouldEqual, `
Ticker | Split+Div Adjusted Close 2019-01-02
------ | -----------------------------------
     A |                               11.00
     B |                              110.00
`)
		})

		Convey("print CSV", func() {
			So(testutil.WriteFile(configFile, fmt.Sprintf(`
{
  "data": {"DB path": "%s", "DB": "%s"},
  "columns": [
    {"kind": "ticker", "sort": "descending"},
    {"kind": "name"},
    {"kind": "exchange"},
    {"kind": "category"},
    {"kind": "sector"},
    {"kind": "industry"},
    {"kind": "price", "date": "2019-01-02"},
    {"kind": "volume", "date": "2019-01-02"}
  ]
}`, tmpdir, dbName)),
				ShouldBeNil)
			flags, err := parseFlags([]string{"-conf", configFile, "-csv"})
			So(err, ShouldBeNil)
			var buf bytes.Buffer
			So(printData(ctx, flags, &buf), ShouldBeNil)
			So("\n"+buf.String(), ShouldEqual, `
Ticker,Name,Exchange,Category,Sector,Industry,Split+Div Adjusted Close 2019-01-02,Cash Volume 2019-01-02
B,Name B,ex B,cat B,sec B,ind B,110.00,1100.00
A,Name A,ex A,cat A,sec A,ind A,11.00,1100.00
`)
		})
	})
}
