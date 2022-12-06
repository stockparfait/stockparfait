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
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stockparfait/logging"
	"github.com/stockparfait/stockparfait/db"

	. "github.com/smartystreets/goconvey/convey"
)

func writeFile(fileName, content string) error {
	f, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = f.Write([]byte(content))
	return err
}

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
		tmpdir, tmpdirErr := ioutil.TempDir("", "testmain")
		defer os.RemoveAll(tmpdir)

		So(tmpdirErr, ShouldBeNil)

		tickersFile := filepath.Join(tmpdir, "tickers.csv")
		pricesFile := filepath.Join(tmpdir, "prices.csv")
		schemaFile := filepath.Join(tmpdir, "schema.json")
		dbName := "testdb"
		args := []string{"-cache", tmpdir, "-db", dbName}

		Convey("import tickers", func() {
			// Add tickers to not-yet-existing DB.
			So(writeFile(tickersFile, `
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

			ctx := context.Background()
			reader := db.NewReader(tmpdir, dbName)
			tickers, err := reader.AllTickerRows(ctx)
			So(err, ShouldBeNil)
			So(tickers, ShouldResemble, expected)

			// Add more tickers to the now-existing DB with a custom schema.
			So(writeFile(tickersFile, `
listed,junk,tkr
TRUE,blah,C
FALSE,foo,D
`[1:]),
				ShouldBeNil)
			So(writeFile(schemaFile, `
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
			tickers, err = reader.AllTickerRows(ctx)
			So(err, ShouldBeNil)
			So(tickers, ShouldResemble, expected)

			// Overwrite tickers in the existing DB.
			So(writeFile(tickersFile, `
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
			tickers, err = reader.AllTickerRows(ctx)
			So(err, ShouldBeNil)
			So(tickers, ShouldResemble, expected)
		})

		Convey("import prices with default reordered schema", func() {
			So(writeFile(pricesFile, `
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
		})

		Convey("import prices with a custom schema", func() {
			So(writeFile(pricesFile, `
listed,price,time,junk,vol*price
TRUE,10,2020-01-01,blah,1000
FALSE,11,2020-01-02,whatever,100
`),
				ShouldBeNil)
			So(writeFile(schemaFile, `
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
	})
}
