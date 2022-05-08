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
	"archive/zip"
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"os"
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

		tickersPage, err := ndl.TestTablePage([][]ndl.Value{
			{
				"SEP", 100, "A", "Name1", "Exch1", "N", "Cat1", "CUSIP1 CUSIP2",
				111, "SICSec1", "SICInd1", "FAMASec1", "FAMAInd1", "Sec1", "Ind1",
				"4 - Mid", "3 - Small", "RT1 RT2", "USD", "Here",
				"2020-02-22", "2000-01-11", "2000-02-01", "2020-02-22",
				"2000-01-01", "2020-03-31", "https://sec.filings", "https://com.site",
			},
			{
				"SFP", 200, "B", "Name2", "Exch2", "Y", "Cat2", "CUSIP3 CUSIP4",
				222, "SICSec2", "SICInd2", "FAMASec2", "FAMAInd2", "Sec2", "Ind2",
				"2 - Micro", "5 - Large", "RT3 RT4", "RUB", "There",
				"2020-02-22", "2000-01-11", "2000-02-01", "2020-02-22",
				"2000-01-01", "2020-03-31", "https://sec.filings", "https://com.site",
			},
			{
				"SFP", 200, "C", "Name3", "Exch3", "Y", "Cat3", "CUSIP5 CUSIP6",
				333, "SICSec3", "SICInd3", "FAMASec3", "FAMAInd3", "Sec3", "Ind3",
				"2 - Micro", "5 - Large", "RT5 RT6", "EUR", "Where",
				"2020-02-22", "2000-01-11", "2000-02-01", "2020-02-22",
				"2000-01-01", "2020-03-31", "https://sec.filings", "https://com.site",
			},
		}, TickerSchema, "")
		So(err, ShouldBeNil)

		Convey("FetchTickers", func() {
			server.ResponseBody = []string{tickersPage}
			ds := NewDataset()

			expected := map[string]db.TickerRow{
				"A": {
					Exchange:    "Exch1",
					Name:        "Name1",
					Category:    "Cat1",
					Sector:      "Sec1",
					Industry:    "Ind1",
					Location:    "Here",
					SECFilings:  "https://sec.filings",
					CompanySite: "https://com.site",
					Active:      true,
				},
				"B": {
					Exchange:    "Exch2",
					Name:        "Name2",
					Category:    "Cat2",
					Sector:      "Sec2",
					Industry:    "Ind2",
					Location:    "There",
					SECFilings:  "https://sec.filings",
					CompanySite: "https://com.site",
					Active:      false,
				},
				"C": {
					Exchange:    "Exch3",
					Name:        "Name3",
					Category:    "Cat3",
					Sector:      "Sec3",
					Industry:    "Ind3",
					Location:    "Where",
					SECFilings:  "https://sec.filings",
					CompanySite: "https://com.site",
					Active:      false,
				},
			}

			Convey("for all tables", func() {
				So(ds.FetchTickers(ctx), ShouldBeNil)
				So(ds.Tickers, ShouldResemble, expected)
			})

			Convey("for selected tables", func() {
				So(ds.FetchTickers(ctx, EquitiesTable), ShouldBeNil)
				So(server.RequestQuery["table"], ShouldResemble, []string{"SEP"})
			})
		})

		actionsPage, err := ndl.TestTablePage([][]ndl.Value{
			{"2001-01-01", "dividend", "A", "Name1", 1.23, "", ""}, // out of order
			{"2000-01-01", "split", "A", "Name1", 2.0, "CT1", "Contra Name1"},
			{"2000-02-01", "listed", "B", "Name2", 0.0, "", ""},
		}, ActionSchema, "")
		So(err, ShouldBeNil)

		Convey("FetchActions", func() {
			server.ResponseBody = []string{actionsPage}
			ds := NewDataset()

			expected := map[string][]Action{
				"A": {
					{
						Date:         db.NewDate(2000, 1, 1),
						Action:       SplitAction,
						Ticker:       "A",
						Name:         "Name1",
						Value:        2.0,
						ContraTicker: "CT1",
						ContraName:   "Contra Name1",
					},
					{
						Date:   db.NewDate(2001, 1, 1),
						Action: DividendAction,
						Ticker: "A",
						Name:   "Name1",
						Value:  1.23,
					},
				},
				"B": {
					{
						Date:   db.NewDate(2000, 2, 1),
						Action: ListedAction,
						Ticker: "B",
						Name:   "Name2",
					},
				},
			}

			Convey("for all actions", func() {
				So(ds.FetchActions(ctx), ShouldBeNil)
				So(ds.RawActions, ShouldResemble, expected)
				So(ds.NumRawActions, ShouldEqual, 3)
			})

			Convey("for selected actions", func() {
				So(ds.FetchActions(ctx, RelevantActions...), ShouldBeNil)
				So(server.RequestQuery["action"], ShouldResemble,
					[]string{"acquisitionby,delisted,dividend,listed,mergerfrom,regulatorydelisting,spinoffdividend,split,voluntarydelisting"})
			})
		})

		bulkJSON := fmt.Sprintf(`{
  "datatable_bulk_download": {
      "file": {
        "link": "%s",
        "status": "regenerating",
        "data_snapshot_time": "2017-04-26 14:33:02 UTC"
      },
      "datatable": {
        "last_refreshed_time": "2017-10-12 09:03:36 UTC"
      }
    }
}`, server.URL()+"/test.zip")
		// The order of the samples is different for the two tickers, to test
		// correct reordering. Ticker "C" is not in Tickers table, to be ignored.
		bulkCSVRaw := `ticker,date,open,high,low,close,volume,closeadj,closeunadj,lastupdated
A,2021-11-09,0.3,0.33,0.3,0.33,7500.0,0.33,0.33,2021-11-09
A,2021-11-08,0.35,0.35,0.35,0.35,10.0,0.35,0.35,2021-11-09
B,2021-09-23,9.95,9.95,10.9,5.0,2692.0,5.0,10.0,2021-09-24
B,2021-09-24,9.74,9.75,9.73,9.75,38502.0,9.75,9.75,2021-09-24
C,2019-09-24,19.74,19.75,19.73,19.75,138502.0,19.75,19.75,2019-09-24
`

		var bulkZip bytes.Buffer
		zipW := zip.NewWriter(&bulkZip)
		w, err := zipW.Create("test.csv")
		So(err, ShouldBeNil)
		_, err = bytes.NewBufferString(bulkCSVRaw).WriteTo(w)
		So(err, ShouldBeNil)
		So(zipW.Close(), ShouldBeNil)
		bulkZipStr := bulkZip.String()

		Convey("BulkDownloadPrices", func() {
			server.ResponseBody = []string{bulkJSON, bulkZipStr}

			expected := map[string][]db.PriceRow{
				"A": {
					{
						Date:               db.NewDate(2021, 11, 8),
						Close:              0.35,
						DollarVolume:       10.0 * 0.35,
						CloseSplitAdjusted: 0.35,
						CloseFullyAdjusted: 0.35,
					},
					{
						Date:               db.NewDate(2021, 11, 9),
						Close:              0.33,
						DollarVolume:       7500.0 * 0.33,
						CloseSplitAdjusted: 0.33,
						CloseFullyAdjusted: 0.33,
					},
				},
				"B": {
					{
						Date:               db.NewDate(2021, 9, 23),
						Close:              10.0,
						DollarVolume:       5.0 * 2692.0,
						CloseSplitAdjusted: 5.0,
						CloseFullyAdjusted: 5.0,
					},
					{
						Date:               db.NewDate(2021, 9, 24),
						Close:              9.75,
						DollarVolume:       9.75 * 38502.0,
						CloseSplitAdjusted: 9.75,
						CloseFullyAdjusted: 9.75,
					},
				},
			}

			ds := NewDataset()
			ds.Tickers["A"] = db.TickerRow{}
			ds.Tickers["B"] = db.TickerRow{}
			So(ds.BulkDownloadPrices(ctx, EquitiesTable), ShouldBeNil)
			So(ds.Prices, ShouldResemble, expected)
		})

		Convey("ComputeActions", func() {
			ds := NewDataset()
			ds.Tickers = map[string]db.TickerRow{
				"A": {Active: false}, // no delisting action, must be added
				"B": {Active: true},
				"C": {Active: true}, // no raw actions, must insert listed action
			}
			ds.RawActions = map[string][]Action{
				"A": {
					// Before the first price, will be recorded at the first price.
					TestAction(db.NewDate(2010, 1, 1), ListedAction, "A", 0.0),
					// Between prices, must merge with the next.
					TestAction(db.NewDate(2020, 1, 15), DividendAction, "A", 1.0),
					TestAction(db.NewDate(2020, 2, 1), SplitAction, "A", 2.0),
				},
				"B": {
					// After the first price.
					TestAction(db.NewDate(2020, 2, 1), DelistedAction, "A", 0.0),
					// After the second but before the last price.
					TestAction(db.NewDate(2020, 2, 15), DelistedAction, "A", 0.0),
					// At the last price, to update the active bit.
					TestAction(db.NewDate(2020, 3, 1), SplitAction, "A", 2.0),
				},
			}
			ds.Prices = map[string][]db.PriceRow{
				"A": {
					db.TestPrice(db.NewDate(2020, 1, 1), 10.0, 5.0, 0.0, true),
					// At split, after dividends.
					db.TestPrice(db.NewDate(2020, 2, 1), 6.0, 6.0, 0.0, true),
					// Delisting action must be at this date.
					db.TestPrice(db.NewDate(2020, 3, 1), 7.0, 7.0, 0.0, true),
				},
				"B": {
					// Before the first action.
					db.TestPrice(db.NewDate(2020, 1, 1), 5.0, 5.0, 0.0, true),
					// At delisted action.
					db.TestPrice(db.NewDate(2020, 2, 1), 6.0, 6.0, 0.0, true),
					// Listed action must be at this date.
					db.TestPrice(db.NewDate(2020, 3, 1), 10.0, 10.0, 0.0, true),
				},
				"C": {
					db.TestPrice(db.NewDate(2021, 1, 1), 5.0, 5.0, 0.0, true),
					db.TestPrice(db.NewDate(2021, 2, 1), 6.0, 6.0, 0.0, true),
					db.TestPrice(db.NewDate(2021, 3, 1), 10.0, 10.0, 0.0, true),
				},
			}
			ds.ComputeActions(ctx)
			So(ds.NumActions, ShouldEqual, 7)
			So(ds.Actions, ShouldResemble, map[string][]db.ActionRow{
				"A": {
					// Listed action at first price.
					db.TestAction(db.NewDate(2020, 1, 1), 1.0, 1.0, true),
					// Merged split + dividend action.
					db.TestAction(db.NewDate(2020, 2, 1), 0.8, 0.5, true),
					// Delisted action - added automatically.
					db.TestAction(db.NewDate(2020, 3, 1), 1.0, 1.0, false),
				},
				"B": {
					// Listed action - added automatically.
					db.TestAction(db.NewDate(2020, 1, 1), 1.0, 1.0, true),
					// Delisted action.
					db.TestAction(db.NewDate(2020, 2, 1), 1.0, 1.0, false),
					// (Re)listed action - added automatically.
					db.TestAction(db.NewDate(2020, 3, 1), 1.0, 0.5, true),
				},
				"C": {
					// Listed action - added automatically.
					db.TestAction(db.NewDate(2021, 1, 1), 1.0, 1.0, true),
				},
			})
			So(ds.Prices["A"][0].Active(), ShouldBeTrue)
			So(ds.Prices["A"][1].Active(), ShouldBeTrue)
			So(ds.Prices["A"][2].Active(), ShouldBeFalse)
			So(ds.Prices["B"][0].Active(), ShouldBeTrue)
			So(ds.Prices["B"][1].Active(), ShouldBeFalse)
			So(ds.Prices["B"][2].Active(), ShouldBeTrue)
			So(ds.Prices["C"][0].Active(), ShouldBeTrue)
			So(ds.Prices["C"][1].Active(), ShouldBeTrue)
			So(ds.Prices["C"][2].Active(), ShouldBeTrue)
		})

		Convey("DownloadAll", func() {
			tmpdir, tmpdirErr := ioutil.TempDir("", "testdownload")
			So(tmpdirErr, ShouldBeNil)
			defer os.RemoveAll(tmpdir)

			server.ResponseBody = []string{
				tickersPage,
				actionsPage,
				bulkJSON,
				bulkZipStr,
			}

			ds := NewDataset()
			So(ds.DownloadAll(ctx, tmpdir, EquitiesTable), ShouldBeNil)
			d := db.NewDatabase(tmpdir)
			meta, err := d.Metadata()
			So(err, ShouldBeNil)
			So(meta, ShouldResemble, db.Metadata{
				Start:      db.NewDate(2019, 9, 24),
				End:        db.NewDate(2021, 11, 9),
				NumTickers: 3,
				NumActions: 4,
				NumPrices:  5,
				NumMonthly: 3,
			})
		})
	})
}
