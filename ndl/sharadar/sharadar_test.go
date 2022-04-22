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

package sharadar

import (
	"context"
	"testing"

	"github.com/stockparfait/fetch"
	"github.com/stockparfait/stockparfait/db"
	"github.com/stockparfait/stockparfait/ndl"

	. "github.com/smartystreets/goconvey/convey"
)

func TestSharadar(t *testing.T) {
	Convey("Dataset API calls work correctly", t, func() {
		server := fetch.NewTestServer()
		defer server.Close()
		server.ResponseBody = []string{"{}"}

		testKey := "testkey"
		ctx := fetch.UseClient(context.Background(), server.Client())
		ndl.URL = server.URL() + "/api/v3"
		ctx = ndl.UseClient(ctx, testKey)

		Convey("FetchTickers", func() {
			page, err := ndl.TestTablePage([][]ndl.Value{
				{
					"TBL1", 100, "T1", "Name1", "Exch1", "N", "Cat1", "CUSIP1 CUSIP2",
					111, "SICSec1", "SICInd1", "FAMASec1", "FAMAInd1", "Sec1", "Ind1",
					"4 - Mid", "3 - Small", "RT1 RT2", "USD", "Here",
					"2020-02-22", "2000-01-11", "2000-02-01", "2020-02-22",
					"2000-01-01", "2020-03-31", "https://sec.filings", "https://com.site",
				},
				{
					"TBL2", 200, "T2", "Name2", "Exch2", "Y", "Cat2", "CUSIP3 CUSIP4",
					222, "SICSec2", "SICInd2", "FAMASec2", "FAMAInd2", "Sec2", "Ind2",
					"2 - Micro", "5 - Large", "RT3 RT4", "RUB", "There",
					"2020-02-22", "2000-01-11", "2000-02-01", "2020-02-22",
					"2000-01-01", "2020-03-31", "https://sec.filings", "https://com.site",
				},
			}, TickerSchema, "")
			So(err, ShouldBeNil)
			server.ResponseBody = []string{page}
			ds := NewDataset()

			expected := map[string]db.TickerRow{
				"T1": {
					Exchange:    "Exch1",
					Name:        "Name1",
					Category:    "Cat1",
					Sector:      "Sec1",
					Industry:    "Ind1",
					Location:    "Here",
					SECFilings:  "https://sec.filings",
					CompanySite: "https://com.site",
				},
				"T2": {
					Exchange:    "Exch2",
					Name:        "Name2",
					Category:    "Cat2",
					Sector:      "Sec2",
					Industry:    "Ind2",
					Location:    "There",
					SECFilings:  "https://sec.filings",
					CompanySite: "https://com.site",
				},
			}

			Convey("for all tables", func() {
				So(ds.FetchTickers(ctx), ShouldBeNil)
				So(ds.Tickers, ShouldResemble, expected)
			})

			Convey("for selected tables", func() {
				So(ds.FetchTickers(ctx, "TBL1"), ShouldBeNil)
				So(server.RequestQuery["table"], ShouldResemble, []string{"TBL1"})
			})
		})

		Convey("FetchActions", func() {
			page, err := ndl.TestTablePage([][]ndl.Value{
				{"2000-01-01", "split", "T1", "Name1", 2.0, "CT1", "Contra Name1"},
				{"2001-01-01", "dividend", "T1", "Name1", 1.23, "", ""},
				{"2000-02-01", "listed", "T2", "Name2", 0.0, "", ""},
			}, ActionSchema, "")
			So(err, ShouldBeNil)
			server.ResponseBody = []string{page}
			ds := NewDataset()

			expected := map[string][]Action{
				"T1": {
					{
						Date:         db.NewDate(2000, 1, 1),
						Action:       SplitAction,
						Ticker:       "T1",
						Name:         "Name1",
						Value:        2.0,
						ContraTicker: "CT1",
						ContraName:   "Contra Name1",
					},
					{
						Date:   db.NewDate(2001, 1, 1),
						Action: DividendAction,
						Ticker: "T1",
						Name:   "Name1",
						Value:  1.23,
					},
				},
				"T2": {
					{
						Date:   db.NewDate(2000, 2, 1),
						Action: ListedAction,
						Ticker: "T2",
						Name:   "Name2",
					},
				},
			}

			Convey("for all actions", func() {
				So(ds.FetchActions(ctx), ShouldBeNil)
				So(ds.RawActions, ShouldResemble, expected)
			})

			Convey("for selected actions", func() {
				So(ds.FetchActions(ctx, SplitAction, DividendAction), ShouldBeNil)
				So(server.RequestQuery["action"], ShouldResemble,
					[]string{"split,dividend"})
			})
		})
	})
}
