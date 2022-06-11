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

package plot

import (
	"testing"

	"github.com/stockparfait/stockparfait/db"
	"github.com/stockparfait/stockparfait/stats"

	. "github.com/smartystreets/goconvey/convey"
)

func TestPlot(t *testing.T) {
	t.Parallel()

	Convey("Plot API works", t, func() {
		c := NewCanvas()
		xyGroup := NewGroup(XYKind, "xy").SetXLogScale(true)
		seriesGroup := NewGroup(SeriesKind, "series")
		So(c.AddGroup(xyGroup), ShouldBeNil)
		So(c.AddGroup(seriesGroup), ShouldBeNil)
		So(c.AddGroup(NewGroup(XYKind, "xy")), ShouldNotBeNil)
		So(c.Groups, ShouldResemble, []*Group{xyGroup, seriesGroup})

		Convey("Adding graphs", func() {
			Convey("to existing groups", func() {
				pdfGraph := NewGraph(XYKind, "p.d.f.").
					SetTitle("Distributions").
					SetXLabel("price").
					SetYLogScale(true)
				timeGraph := NewGraph(SeriesKind, "time")

				So(pdfGraph.Title, ShouldEqual, "Distributions")
				So(pdfGraph.XLabel, ShouldEqual, "price")
				So(pdfGraph.YLogScale, ShouldBeTrue)

				So(c.AddGraph(pdfGraph, "xy"), ShouldBeNil)
				So(c.AddGraph(timeGraph, "series"), ShouldBeNil)
				So(c.AddGraph(NewGraph(SeriesKind, "wrong"), "xy"), ShouldNotBeNil)
				// Duplicate graph name in another group.
				So(c.AddGraph(NewGraph(XYKind, "time"), "xy"), ShouldNotBeNil)
				So(len(c.graphMap), ShouldEqual, 2)
				So(c.Groups[0].Graphs[0], ShouldEqual, pdfGraph)
			})

			Convey("to a new group", func() {
				g := NewGraph(XYKind, "scatter")
				So(c.AddGraph(g, "dots"), ShouldBeNil)
				So(len(c.Groups), ShouldEqual, 3)
				So(c.Groups[:2], ShouldResemble, []*Group{xyGroup, seriesGroup})
				So(c.Groups[2].Graphs[0], ShouldEqual, g)
			})

			Convey("from a group pre-populated with graphs", func() {
				gr := NewGroup(XYKind, "prep")
				g1 := NewGraph(XYKind, "one")
				g2 := NewGraph(XYKind, "two")

				So(gr.AddGraph(g1), ShouldBeNil)
				So(gr.AddGraph(g2), ShouldBeNil)
				So(gr.AddGraph(NewGraph(XYKind, "one")), ShouldNotBeNil)

				So(c.AddGroup(gr), ShouldBeNil)
				So(c.Groups, ShouldResemble, []*Group{xyGroup, seriesGroup, gr})
				So(len(c.graphMap), ShouldEqual, 2)
			})
		})

		Convey("Adding plots", func() {
			So(c.AddGraph(NewGraph(XYKind, "lines"), "xy"), ShouldBeNil)
			So(c.AddGraph(NewGraph(SeriesKind, "prices"), "time"), ShouldBeNil)

			x := []float64{1.0, 2.0, 3.0}
			y := []float64{10.0, 20.0, 30.0}
			dates := []db.Date{
				db.NewDate(2020, 1, 1),
				db.NewDate(2020, 1, 2),
				db.NewDate(2020, 1, 3),
			}
			ts := stats.NewTimeseries().Init(dates, y)

			xyPlot := NewXYPlot(x, y).
				SetYLabel("p").
				SetLegend("PDF").
				SetChartType(ChartDashed)

			timePlot := NewSeriesPlot(ts)

			Convey("Plot access methods", func() {
				x2, y2 := xyPlot.GetXY()
				So(x2, ShouldResemble, x)
				So(y2, ShouldResemble, y)
				So(func() { xyPlot.GetTimeseries() }, ShouldPanic)

				So(func() { timePlot.GetXY() }, ShouldPanic)
				So(timePlot.GetTimeseries(), ShouldResemble, ts)
			})

			Convey("to the right Y axis", func() {
				So(c.AddPlotRight(xyPlot, "lines"), ShouldBeNil)
				So(c.AddPlotRight(timePlot, "prices"), ShouldBeNil)
				So(c.AddPlotRight(timePlot, "lines"), ShouldNotBeNil)
				So(c.AddPlotRight(xyPlot, "nonexistent"), ShouldNotBeNil)

				So(c.graphMap["lines"].PlotsRight, ShouldResemble, []*Plot{xyPlot})
				So(c.graphMap["prices"].PlotsRight, ShouldResemble, []*Plot{timePlot})
			})

			Convey("to the left Y axis", func() {
				So(c.AddPlotLeft(xyPlot, "lines"), ShouldBeNil)
				So(c.AddPlotLeft(timePlot, "prices"), ShouldBeNil)
				So(c.AddPlotLeft(timePlot, "lines"), ShouldNotBeNil)
				So(c.AddPlotLeft(xyPlot, "nonexistent"), ShouldNotBeNil)

				So(c.graphMap["lines"].PlotsLeft, ShouldResemble, []*Plot{xyPlot})
				So(c.graphMap["prices"].PlotsLeft, ShouldResemble, []*Plot{timePlot})
			})
		})
	})
}
