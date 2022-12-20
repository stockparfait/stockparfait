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
	"math"
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

	Convey("parseFlags", t, func() {
		Convey("-tickers", func() {
			flags, err := parseFlags([]string{
				"-cache", "path/to/cache", "-db", "name",
				"-log-level", "warning", "-tickers", "tickers.csv"})
			So(err, ShouldBeNil)
			So(flags.DBDir, ShouldEqual, "path/to/cache")
			So(flags.DBName, ShouldEqual, "name")
			So(flags.LogLevel, ShouldEqual, logging.Warning)
			So(flags.Tickers, ShouldEqual, "tickers.csv")
			So(flags.Prices, ShouldEqual, "")
			So(flags.UpdateMetadata, ShouldBeFalse)
		})

		Convey("-prices with -ticker", func() {
			flags, err := parseFlags([]string{
				"-db", "name", "-prices", "prices.csv", "-ticker", "ABC"})
			So(err, ShouldBeNil)
			So(flags.DBName, ShouldEqual, "name")
			So(flags.LogLevel, ShouldEqual, logging.Info)
			So(flags.Prices, ShouldEqual, "prices.csv")
			So(flags.Ticker, ShouldEqual, "ABC")
			So(flags.Tickers, ShouldEqual, "")
			So(flags.UpdateMetadata, ShouldBeFalse)
		})

		Convey("-prices without -ticker", func() {
			_, err := parseFlags([]string{"-db", "name", "-prices", "prices.csv"})
			So(err, ShouldNotBeNil)
		})

		Convey("-update-metada", func() {
			flags, err := parseFlags([]string{"-db", "name", "-update-metadata"})
			So(err, ShouldBeNil)
			So(flags.DBName, ShouldEqual, "name")
			So(flags.Prices, ShouldEqual, "")
			So(flags.Tickers, ShouldEqual, "")
			So(flags.UpdateMetadata, ShouldBeTrue)
		})

		Convey("Incompatible flags", func() {
			_, err := parseFlags([]string{
				"-db", "name", "-prices", "prices.csv", "-update-metadata"})
			So(err, ShouldNotBeNil)
		})
	})

	Convey("run works", t, func() {
		tmpdir, tmpdirErr := os.MkdirTemp("", "testmain")
		defer os.RemoveAll(tmpdir)

		So(tmpdirErr, ShouldBeNil)

		tickersFile := filepath.Join(tmpdir, "tickers.csv")
		pricesFile := filepath.Join(tmpdir, "prices.csv")
		pricesFile2 := filepath.Join(tmpdir, "prices2.csv")
		schemaFile := filepath.Join(tmpdir, "schema.json")
		dbName := "testdb"
		args := []string{"-cache", tmpdir, "-db", dbName}

		Convey("import tickers", func() {
			// Add tickers to not-yet-existing DB.
			So(testutil.WriteFile(tickersFile, `
Ticker,Source,Exchange,Name,Category,Sector,Industry,Location,SEC Filings,Company Site,Active
ABC,TEST,Exch,ABC Co.,Cat,Sec,Ind,Over Here,sec.gov,abc.com,TRUE
CBA,TEST,Exch2,CBA Co.,Cat2,Sec2,Ind2,Over There,sec.gov,cba.com,FALSE
`),
				ShouldBeNil)
			So(run(append(args, "-tickers", tickersFile)), ShouldBeNil)

			expected := map[string]db.TickerRow{
				"ABC": {
					Source:      "TEST",
					Exchange:    "Exch",
					Name:        "ABC Co.",
					Category:    "Cat",
					Sector:      "Sec",
					Industry:    "Ind",
					Location:    "Over Here",
					SECFilings:  "sec.gov",
					CompanySite: "abc.com",
					Active:      true,
				},
				"CBA": {
					Source:      "TEST",
					Exchange:    "Exch2",
					Name:        "CBA Co.",
					Category:    "Cat2",
					Sector:      "Sec2",
					Industry:    "Ind2",
					Location:    "Over There",
					SECFilings:  "sec.gov",
					CompanySite: "cba.com",
					Active:      false,
				},
			}

			reader := db.NewReader(tmpdir, dbName)
			tickers, err := reader.AllTickerRows()
			So(err, ShouldBeNil)
			So(tickers, ShouldResemble, expected)

			// Add more tickers to the now-existing DB with a custom schema.
			So(testutil.WriteFile(tickersFile, `
listed,junk,tkr
TRUE,blah,C
FALSE,foo,D
`[1:]),
				ShouldBeNil)
			So(testutil.WriteFile(schemaFile, `
{
  "Ticker": "tkr",
  "Active": "listed"
}
`),
				ShouldBeNil)
			So(run(append(args, "-tickers", tickersFile, "-schema", schemaFile)),
				ShouldBeNil)
			expected["C"] = db.TickerRow{Active: true}
			expected["D"] = db.TickerRow{Active: false}
			reader = db.NewReader(tmpdir, dbName)
			tickers, err = reader.AllTickerRows()
			So(err, ShouldBeNil)
			So(tickers, ShouldResemble, expected)

			// Overwrite tickers in the existing DB.
			So(testutil.WriteFile(tickersFile, `
listed,tkr
TRUE,X
t,Y
`[1:]),
				ShouldBeNil)
			So(run(append(args, "-tickers", tickersFile, "-schema",
				schemaFile, "-replace")), ShouldBeNil)
			expected = map[string]db.TickerRow{
				"X": {Active: true},
				"Y": {Active: true},
			}
			reader = db.NewReader(tmpdir, dbName)
			tickers, err = reader.AllTickerRows()
			So(err, ShouldBeNil)
			So(tickers, ShouldResemble, expected)
		})

		Convey("import prices with default reordered schema", func() {
			So(testutil.WriteFile(pricesFile, `
Active,Close,Close split adj,Date,Close fully adj,Cash Volume
TRUE,10,5,2020-01-01,4.5,1000
FALSE,11,5.5,2020-01-02,4.6,100
`),
				ShouldBeNil)
			So(run(append(args, "-prices", pricesFile, "-ticker", "A")), ShouldBeNil)
			expected := []db.PriceRow{
				db.TestPrice(db.NewDate(2020, 1, 1), 10, 5, 4.5, 1000, true),
				db.TestPrice(db.NewDate(2020, 1, 2), 11, 5.5, 4.6, 100, false),
			}
			reader := db.NewReader(tmpdir, dbName)
			prices, err := reader.Prices("A")
			So(err, ShouldBeNil)
			So(prices, ShouldResemble, expected)
			monthly, err := reader.Monthly("A", db.Date{}, db.Date{})
			So(err, ShouldBeNil)
			So(len(monthly), ShouldEqual, 1)
			monthly[0].SumAbsLogProfits = float32(
				testutil.Round(float64(monthly[0].SumAbsLogProfits), 5))
			expSALP := float32(testutil.Round(math.Log(4.6)-math.Log(4.5), 5))
			So(monthly, ShouldResemble, []db.ResampledRow{{
				Open:               10,
				OpenSplitAdjusted:  5,
				OpenFullyAdjusted:  4.5,
				Close:              11,
				CloseSplitAdjusted: 5.5,
				CloseFullyAdjusted: 4.6,
				CashVolume:         1100,
				DateOpen:           db.NewDate(2020, 1, 1),
				DateClose:          db.NewDate(2020, 1, 2),
				SumAbsLogProfits:   expSALP,
				NumSamples:         2,
				Active:             false,
			}})
		})

		Convey("import prices with a custom schema, ignore invalid values", func() {
			So(testutil.WriteFile(pricesFile, `
listed,price,time,junk,vol*price
TRUE,10,2020-01-01,blah,1000
FALSE,11,2020-01-02,whatever,100
FALSE,NaN,2020-01-03,ignored,100
FALSE,11,2020-01-04,ignored,Inf
`),
				ShouldBeNil)
			So(testutil.WriteFile(schemaFile, `
{
  "Date": "time",
  "Active": "listed",
  "Close": "price",
  "Close split adj": "price",
  "Close fully adj": "price",
  "Cash Volume": "vol*price"
}
`),
				ShouldBeNil)
			So(run(append(args, "-prices", pricesFile, "-ticker", "A",
				"-schema", schemaFile)), ShouldBeNil)
			expected := []db.PriceRow{
				db.TestPrice(db.NewDate(2020, 1, 1), 10, 10, 10, 1000, true),
				db.TestPrice(db.NewDate(2020, 1, 2), 11, 11, 11, 100, false),
			}
			reader := db.NewReader(tmpdir, dbName)
			prices, err := reader.Prices("A")
			So(err, ShouldBeNil)
			So(prices, ShouldResemble, expected)
		})

		Convey("update metadata", func() {
			So(testutil.WriteFile(tickersFile, `
Ticker
A
B
IGNORED
`),
				ShouldBeNil)
			So(testutil.WriteFile(pricesFile, `
Date,Close fully adj
2020-01-02,10
2020-02-02,11
`),
				ShouldBeNil)
			So(testutil.WriteFile(pricesFile2, `
Date,Close fully adj
2020-01-05,10
2020-03-10,11
`),
				ShouldBeNil)
			So(run(append(args, "-tickers", tickersFile)), ShouldBeNil)
			So(run(append(args, "-prices", pricesFile, "-ticker", "A")), ShouldBeNil)
			So(run(append(args, "-prices", pricesFile2, "-ticker", "B")), ShouldBeNil)
			So(run(append(args, "-update-metadata")), ShouldBeNil)

			reader := db.NewReader(tmpdir, dbName)
			m, err := reader.Metadata()
			So(err, ShouldBeNil)
			So(m, ShouldResemble, db.Metadata{
				Start:      db.NewDate(2020, 1, 2),
				End:        db.NewDate(2020, 3, 10),
				NumTickers: 2,
				NumPrices:  4,
				NumMonthly: 4,
			})
		})

		Convey("cleanup", func() {
			So(testutil.WriteFile(tickersFile, `
Ticker
A
`),
				ShouldBeNil)
			So(testutil.WriteFile(pricesFile, `
Date,Close fully adj
2020-01-02,10
`),
				ShouldBeNil)
			So(testutil.WriteFile(pricesFile2, `
Date,Close fully adj
2020-01-05,10
`),
				ShouldBeNil)
			So(run(append(args, "-tickers", tickersFile)), ShouldBeNil)
			So(run(append(args, "-prices", pricesFile, "-ticker", "A")), ShouldBeNil)
			So(run(append(args, "-prices", pricesFile2, "-ticker", "B")), ShouldBeNil)

			bFile := filepath.Join(tmpdir, dbName, "prices", "B.gob")
			So(testutil.FileExists(bFile), ShouldBeTrue)

			So(run(append(args, "-cleanup")), ShouldBeNil)
			So(testutil.FileExists(bFile), ShouldBeFalse)
		})
	})
}
