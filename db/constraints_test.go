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
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestConstraints(t *testing.T) {
	t.Parallel()

	Convey("Constraints work correctly", t, func() {
		beforeStart := NewDate(2020, 1, 1)
		start := NewDate(2020, 2, 1)
		end := NewDate(2020, 11, 31)
		afterEnd := NewDate(2021, 1, 1)
		tc := NewConstraints().ExcludeTicker("E").Ticker("A", "B", "E")
		tc = tc.Exchange("NASDAQ", "NYSE")
		tc = tc.Name("Fat Ducks", "Plumb & Plumber")
		tc = tc.Category("Do", "Break")
		tc = tc.Sector("Domestic", "Foreign")
		tc = tc.Industry("Food", "Waste")
		tc = tc.StartAt(start).EndAt(end)

		Convey("CheckTicker", func() {
			So(tc.CheckTicker("A"), ShouldBeTrue)
			So(tc.CheckTicker("B"), ShouldBeTrue)
			So(tc.CheckTicker("E"), ShouldBeFalse)
			So(tc.CheckTicker("UNKNOWN"), ShouldBeFalse)
		})

		Convey("CheckTickerRow", func() {
			ticker := TickerRow{
				Exchange: "NASDAQ",
				Name:     "Fat Ducks",
				Category: "Do",
				Sector:   "Domestic",
				Industry: "Food",
			}
			So(tc.CheckTickerRow(ticker), ShouldBeTrue)
			ticker.Name = "Dumb & Dumber"
			So(tc.CheckTickerRow(ticker), ShouldBeFalse)
			ticker.Name = "Plumb & Plumber"
			So(tc.CheckTickerRow(ticker), ShouldBeTrue)
			ticker.Category = "Idle"
			So(tc.CheckTickerRow(ticker), ShouldBeFalse)
			ticker.Category = "Break"
			So(tc.CheckTickerRow(ticker), ShouldBeTrue)
			ticker.Sector = "Weird"
			So(tc.CheckTickerRow(ticker), ShouldBeFalse)
			ticker.Sector = "Foreign"
			So(tc.CheckTickerRow(ticker), ShouldBeTrue)
			ticker.Exchange = "DarkPool"
			So(tc.CheckTickerRow(ticker), ShouldBeFalse)
			ticker.Exchange = "NYSE"
			So(tc.CheckTickerRow(ticker), ShouldBeTrue)
			ticker.Industry = "Garbage"
			So(tc.CheckTickerRow(ticker), ShouldBeFalse)
			ticker.Industry = "Waste"
			So(tc.CheckTickerRow(ticker), ShouldBeTrue)
		})

		Convey("CheckAction", func() {
			So(tc.CheckAction(TestAction(start, 1.0, 1.0, true)), ShouldBeTrue)
			So(tc.CheckAction(TestAction(end, 1.0, 1.0, true)), ShouldBeTrue)
			So(tc.CheckAction(TestAction(beforeStart, 1.0, 1.0, true)), ShouldBeFalse)
			So(tc.CheckAction(TestAction(afterEnd, 1.0, 1.0, true)), ShouldBeFalse)
		})

		Convey("CheckPrice", func() {
			So(tc.CheckPrice(TestPrice(start, 10.0, 10.0, 100.0, true)), ShouldBeTrue)
			So(tc.CheckPrice(TestPrice(afterEnd, 10.0, 10.0, 100.0, true)), ShouldBeFalse)
		})

		Convey("CheckResampled", func() {
			So(tc.CheckResampled(ResampledRow{DateOpen: start, DateClose: end}), ShouldBeTrue)
			So(tc.CheckResampled(ResampledRow{DateOpen: beforeStart, DateClose: end}), ShouldBeFalse)
			So(tc.CheckResampled(ResampledRow{DateOpen: start, DateClose: afterEnd}), ShouldBeFalse)
		})
	})
}
