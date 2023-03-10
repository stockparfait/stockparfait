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
	"strings"
	"testing"

	"github.com/stockparfait/testutil"

	. "github.com/smartystreets/goconvey/convey"
)

func TestCSV(t *testing.T) {
	t.Parallel()

	Convey("ReadCSVTicker works", t, func() {
		Convey("with default schema", func() {
			c := NewTickerRowConfig()
			csvRows := strings.NewReader(strings.Join(TickerRowHeader(), ",") + `
A,test,exch,A Co.,cat,sec,ind,loc,secf,www,FALSE
B,test,exch2,B Co.,cat2,sec2,ind2,loc2,secf2,www2,TRUE
`)
			tickers := make(map[string]TickerRow)
			So(ReadCSVTickers(csvRows, c, tickers), ShouldBeNil)
			So(tickers, ShouldResemble, map[string]TickerRow{
				"A": {
					Source:      "test",
					Exchange:    "exch",
					Name:        "A Co.",
					Category:    "cat",
					Sector:      "sec",
					Industry:    "ind",
					Location:    "loc",
					SECFilings:  "secf",
					CompanySite: "www",
					Active:      false,
				},
				"B": {
					Source:      "test",
					Exchange:    "exch2",
					Name:        "B Co.",
					Category:    "cat2",
					Sector:      "sec2",
					Industry:    "ind2",
					Location:    "loc2",
					SECFilings:  "secf2",
					CompanySite: "www2",
					Active:      true,
				},
			})
		})

		Convey("headless with custom schema (head in config)", func() {
			cfgJSON := testutil.JSON(`
{
  "Name": "long name",
  "Active": "listed",
  "Ticker": "short name",
  "header": ["listed", "long name", "unused", "short name", "Category", "whatever"]
}`)
			var c TickerRowConfig
			So(c.InitMessage(cfgJSON), ShouldBeNil)

			// Custom names and reordered columns.
			csvRows := strings.NewReader(`
FALSE,A Co.,blah,A,cat,more blah
TRUE,B Co.,blah,B,cat2,more blah
`[1:])
			tickers := map[string]TickerRow{"ORIG": {Name: "Original"}}
			So(ReadCSVTickers(csvRows, &c, tickers), ShouldBeNil)
			So(tickers, ShouldResemble, map[string]TickerRow{
				"ORIG": {Name: "Original"}, // should preserve
				"A": {
					Name:     "A Co.",
					Category: "cat",
					Active:   false,
				},
				"B": {
					Name:     "B Co.",
					Category: "cat2",
					Active:   true,
				},
			})
		})
	})

	Convey("ReadCSVPrices works", t, func() {
		Convey("with default schema", func() {
			c := NewPriceRowConfig()
			csvRows := strings.NewReader(strings.Join(PriceRowHeader(), ",") + `
2020-01-01,10.2,5.2,5.1,5.1,5.1,5.1,1000,TRUE
2020-01-02,20.2,10.2,10.1,10.1,10.1,10.1,2000,FALSE
`)
			prices, err := ReadCSVPrices(csvRows, c)
			So(err, ShouldBeNil)
			So(prices, ShouldResemble, []PriceRow{
				TestPrice(NewDate(2020, 1, 1), 10.2, 5.2, 5.1, 1000, true),
				TestPrice(NewDate(2020, 1, 2), 20.2, 10.2, 10.1, 2000, false),
			})
		})

		Convey("headless with custom schema, unsorted", func() {
			// Map "eod" to all three prices; skip Active and CashVolume.
			cfgJSON := testutil.JSON(`
{
  "Date":            "time",
  "Close":           "eod",
  "Close split adj": "eod",
  "Open fully adj":  "eod",
  "High fully adj":  "eod",
  "Low fully adj":   "eod",
  "Close fully adj": "eod",
  "header": ["eod", "time"]
}`)
			var c PriceRowConfig
			So(c.InitMessage(cfgJSON), ShouldBeNil)
			csvRows := strings.NewReader(`
11.2,2020-01-02
10,2020-01-01
`[1:])
			prices, err := ReadCSVPrices(csvRows, &c)
			So(err, ShouldBeNil)
			So(prices, ShouldResemble, []PriceRow{
				TestPrice(NewDate(2020, 1, 1), 10, 10, 10, 0, true),
				TestPrice(NewDate(2020, 1, 2), 11.2, 11.2, 11.2, 0, true),
			})
		})
	})
}
