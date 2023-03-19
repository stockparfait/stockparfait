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
	"os"
	"testing"

	"github.com/stockparfait/logging"
	"github.com/stockparfait/stockparfait/db"

	. "github.com/smartystreets/goconvey/convey"
)

func TestMain(t *testing.T) {
	t.Parallel()

	tmpdir, tmpdirErr := os.MkdirTemp("", "test_list_app")
	defer os.RemoveAll(tmpdir)

	Convey("Setup succeeded", t, func() {
		So(tmpdirErr, ShouldBeNil)
	})

	Convey("parseFlags", t, func() {
		flags, err := parseFlags([]string{
			"-cache", "path/to/cache", "-db", "name",
			"-log-level", "warning", "-tickers"})
		So(err, ShouldBeNil)
		So(flags.DBDir, ShouldEqual, "path/to/cache")
		So(flags.DBName, ShouldEqual, "name")
		So(flags.LogLevel, ShouldEqual, logging.Warning)
		So(flags.Tickers, ShouldBeTrue)
	})

	Convey("printData works", t, func() {
		dbName := "testdb"
		tickers := map[string]db.TickerRow{
			"A": {Source: "test", Active: true},
			"B": {Source: "test", Active: false},
		}
		pricesA := []db.PriceRow{
			db.TestPrice(db.NewDate(2019, 1, 1), 10.0, 10.0, 10.0, 1000.0, true),
			db.TestPrice(db.NewDate(2019, 1, 2), 11.0, 11.0, 11.0, 1100.0, true),
			db.TestPrice(db.NewDate(2019, 1, 3), 12.0, 12.0, 12.0, 1200.0, false),
		}
		monthly := map[string][]db.ResampledRow{
			"A": {
				db.TestResampled(db.NewDate(2019, 1, 1), db.NewDate(2019, 1, 31), 10.0, 10.0, 10.0, 1000.0, true),
				db.TestResampled(db.NewDate(2019, 2, 1), db.NewDate(2019, 2, 28), 10.0, 10.0, 10.0, 1000.0, false),
			},
		}
		w := db.NewWriter(tmpdir, dbName)
		So(w.WriteTickers(tickers), ShouldBeNil)
		So(w.WritePrices("A", pricesA), ShouldBeNil)
		So(w.WriteMonthly(monthly), ShouldBeNil)
		So(w.WriteMetadata(w.Metadata), ShouldBeNil)

		ctx := context.Background()

		Convey("tickers", func() {
			flags, err := parseFlags([]string{"-cache", tmpdir, "-db", dbName,
				"-tickers", "-csv"})
			So(err, ShouldBeNil)
			var buf bytes.Buffer
			So(printData(ctx, flags, &buf), ShouldBeNil)
			So("\n"+buf.String(), ShouldEqual, `
Ticker,Source,Exchange,Name,Category,Sector,Industry,Location,SEC Filings,Company Site,Active
A,test,,,,,,,,,TRUE
B,test,,,,,,,,,FALSE
`)
		})

		Convey("prices", func() {
			flags, err := parseFlags([]string{"-cache", tmpdir, "-db", dbName,
				"-prices", "A", "-csv"})
			So(err, ShouldBeNil)
			var buf bytes.Buffer
			So(printData(ctx, flags, &buf), ShouldBeNil)
			So("\n"+buf.String(), ShouldEqual, `
Date,Open,High,Low,Close,Close split adj,Close fully adj,Cash Volume,Active
2019-01-01,10,10,10,10,10,10,1000,TRUE
2019-01-02,11,11,11,11,11,11,1100,TRUE
2019-01-03,12,12,12,12,12,12,1200,FALSE
`)
		})

		Convey("monthly", func() {
			flags, err := parseFlags([]string{"-cache", tmpdir, "-db", dbName,
				"-monthly", "A", "-csv"})
			So(err, ShouldBeNil)
			var buf bytes.Buffer
			So(printData(ctx, flags, &buf), ShouldBeNil)
			So("\n"+buf.String(), ShouldEqual, `
Open,Open split adj,Open fully adj,Close,Close split adj,Close fully adj,Cash Volume,Date Open,Date Close,Sum Abs Log Profits,Samples,Active
10,10,10,10,10,10,1000,2019-01-01,2019-01-31,0.2,20,TRUE
10,10,10,10,10,10,1000,2019-02-01,2019-02-28,0.2,20,FALSE
`)
		})
	})
}
