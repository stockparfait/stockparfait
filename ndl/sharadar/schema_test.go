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
	"testing"

	"github.com/stockparfait/stockparfait/db"
	"github.com/stockparfait/stockparfait/ndl"

	. "github.com/smartystreets/goconvey/convey"
)

func TestSchema(t *testing.T) {
	Convey("Ticker.Load works", t, func() {
		realSchema := ndl.Schema{
			{Name: "permaticker", Type: "Integer"},
			{Name: "table", Type: "text"},       // reordered
			{Name: "extrafield", Type: "Weird"}, // extra field
			{Name: "ticker", Type: "text"},
			{Name: "name", Type: "text"},
			{Name: "exchange", Type: "text"},
			{Name: "isdelisted", Type: "text"},
			{Name: "category", Type: "text"},
			{Name: "cusips", Type: "text"},
			{Name: "siccode", Type: "Integer"},
			{Name: "sicsector", Type: "text"},
			{Name: "sicindustry", Type: "text"},
			{Name: "famasector", Type: "text"},
			{Name: "famaindustry", Type: "text"},
			{Name: "sector", Type: "text"},
			{Name: "industry", Type: "text"},
			{Name: "scalemarketcap", Type: "text"},
			{Name: "scalerevenue", Type: "text"},
			{Name: "relatedtickers", Type: "text"},
			{Name: "currency", Type: "text"},
			{Name: "location", Type: "text"},
			{Name: "lastupdated", Type: "Date"},
			{Name: "firstadded", Type: "Date"},
			{Name: "firstpricedate", Type: "Date"},
			{Name: "lastpricedate", Type: "Date"},
			{Name: "firstquarter", Type: "text"},
			{Name: "lastquarter", Type: "text"},
			{Name: "secfilings", Type: "text"},
			{Name: "companysite", Type: "text"},
		}
		data := []ndl.Value{
			123.0,
			"SEP",
			"Extra Field",
			"ABC",
			"Fake Company",
			"NYSE",
			"Y",
			"Fake Category",
			"cusips blah",
			42.0, // siccode
			"sicsectorTest",
			"sicindustryTest",
			"FAMASector",
			"FAMAIndustry",
			nil, // missing sector
			"Fake Industry",
			"4 - Mid",  // market cap
			"1 - Nano", // revenue
			"T1 T2",    // related tickers
			"USD",
			"Over Here",  // location
			"2020-04-14", // last updated
			"2000-01-01", // first added
			"2001-03-14", // first price date
			"2019-12-31", // last price date
			"2001-03-01", // first quarter
			"2019-12-31", // last quarter
			"https://sec.filings",
			"https://site.com",
		}
		row := Ticker{}
		So(row.Load(data, realSchema), ShouldBeNil)
		So(row, ShouldResemble, Ticker{
			TableName:      "SEP",
			Permaticker:    123,
			Ticker:         "ABC",
			Name:           "Fake Company",
			Exchange:       "NYSE",
			IsDelisted:     true,
			Category:       "Fake Category",
			CUSIPs:         []string{"cusips", "blah"},
			SICCode:        42,
			SICSector:      "sicsectorTest",
			SICIndustry:    "sicindustryTest",
			FAMASector:     "FAMASector",
			FAMAIndustry:   "FAMAIndustry",
			Sector:         "",
			Industry:       "Fake Industry",
			ScaleMarketCap: ScaleMid,
			ScaleRevenue:   ScaleNano,
			RelatedTickers: []string{"T1", "T2"},
			Currency:       "USD",
			Location:       "Over Here",
			LastUpdated:    db.NewDate(2020, 4, 14),
			FirstAdded:     db.NewDate(2000, 1, 1),
			FirstPriceDate: db.NewDate(2001, 3, 14),
			LastPriceDate:  db.NewDate(2019, 12, 31),
			FirstQuarter:   db.NewDate(2001, 3, 1),
			LastQuarter:    db.NewDate(2019, 12, 31),
			SECFilings:     "https://sec.filings",
			CompanySite:    "https://site.com",
		})
	})

	Convey("Action works", t, func() {
		Convey("ActionType.String()", func() {
			a := SplitAction
			So(a.String(), ShouldEqual, "split")
			a = DividendAction
			So(a.String(), ShouldEqual, "dividend")
		})

		Convey("Load", func() {
			realSchema := ndl.Schema{
				{Name: "action", Type: "text"},
				{Name: "date", Type: "Date"}, // reordered
				{Name: "ticker", Type: "text"},
				{Name: "name", Type: "text"},
				{Name: "extraField", Type: "text"},
				{Name: "value", Type: "BigDecimal(20,5)"},
				{Name: "contraticker", Type: "text"}, // old/new ticker name
				{Name: "contraname", Type: "text"},   // old/new company name
			}
			data := []ndl.Value{
				"split",
				"2019-11-12",
				"ABC",
				"Fake Company",
				"Extra Field",
				0.25,
				"TKR", // contraticker
				"Contra Name",
			}
			row := Action{}
			So(row.Load(data, realSchema), ShouldBeNil)
			So(row, ShouldResemble, Action{
				Date:         db.NewDate(2019, 11, 12),
				Action:       SplitAction,
				Ticker:       "ABC",
				Name:         "Fake Company",
				Value:        0.25,
				ContraTicker: "TKR",
				ContraName:   "Contra Name",
			})
			So(row.Is(DividendAction, SplitAction), ShouldBeTrue)
			So(row.Is(MergerToAction), ShouldBeFalse)
		})
	})

	Convey("Price.Load works", t, func() {
		realSchema := ndl.Schema{
			{Name: "date", Type: "Date"},
			{Name: "ticker", Type: "text"}, // reordered
			{Name: "open", Type: "double"},
			{Name: "high", Type: "double"},
			{Name: "low", Type: "double"},
			{Name: "close", Type: "double"},
			{Name: "volume", Type: "double"},
			{Name: "closeunadj", Type: "double"},
			{Name: "closeadj", Type: "double"},
			{Name: "lastupdated", Type: "Date"},
			{Name: "fakefield", Type: "text"}, // extra field
		}
		data := []ndl.Value{
			"2020-03-14",
			"ABC",
			14.5,
			20.0,
			10.2,
			15.0,
			1234.0,
			30.0,
			10.0,
			"2020-04-01",
			"fake",
		}
		row := Price{}
		So(row.Load(data, realSchema), ShouldBeNil)
		So(row, ShouldResemble, Price{
			Ticker:          "ABC",
			Date:            db.NewDate(2020, 3, 14),
			Open:            14.5,
			High:            20.0,
			Low:             10.2,
			Close:           15.0,
			Volume:          1234.0,
			CloseUnadjusted: 30.0,
			CloseAdjusted:   10.0,
			LastUpdated:     db.NewDate(2020, 4, 1),
		})
	})

	Convey("Price.FromCSV works", t, func() {
		header := []string{
			"date",
			"ticker", // reordered
			"open",
			"high",
			"low",
			"close",
			"volume",
			"closeunadj",
			"closeadj",
			"lastupdated",
			"fakefield", // extra field
		}
		cmap, err := PriceSchema.MapCSVColumns(header)
		So(err, ShouldBeNil)

		Convey("for a regular row", func() {
			row := []string{
				"2020-03-14",
				"ABC",
				"14.5",
				"20.0",
				"10.2",
				"15.0",
				"1234.0",
				"30.0",
				"10.0",
				"2020-04-01",
				"fake",
			}
			p := Price{}
			So(p.FromCSV(row, cmap), ShouldBeNil)
			So(p, ShouldResemble, Price{
				Ticker:          "ABC",
				Date:            db.NewDate(2020, 3, 14),
				Open:            14.5,
				High:            20.0,
				Low:             10.2,
				Close:           15.0,
				Volume:          1234.0,
				CloseUnadjusted: 30.0,
				CloseAdjusted:   10.0,
				LastUpdated:     db.NewDate(2020, 4, 1),
			})
		})

		Convey("for a row with a missing volume", func() {
			row := []string{
				"2020-03-14",
				"ABC",
				"14.5",
				"20.0",
				"10.2",
				"15.0",
				"", // volume missing (happens in real data)
				"30.0",
				"10.0",
				"2020-04-01",
				"fake",
			}
			p := Price{}
			So(p.FromCSV(row, cmap), ShouldBeNil)
			So(p, ShouldResemble, Price{
				Ticker:          "ABC",
				Date:            db.NewDate(2020, 3, 14),
				Open:            14.5,
				High:            20.0,
				Low:             10.2,
				Close:           15.0,
				Volume:          0.0,
				CloseUnadjusted: 30.0,
				CloseAdjusted:   10.0,
				LastUpdated:     db.NewDate(2020, 4, 1),
			})
		})
	})
}
