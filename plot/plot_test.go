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
	"context"
	"testing"

	"github.com/stockparfait/stockparfait/db"
	"github.com/stockparfait/stockparfait/stats"

	. "github.com/smartystreets/goconvey/convey"
)

func TestPlot(t *testing.T) {
	t.Parallel()

	Convey("Plot API works", t, func() {
		c := NewCanvas()
		ctx := Use(context.Background(), c)
		xyGroup := NewGroup(XYKind, "xy").SetXLogScale(true)
		seriesGroup := NewGroup(SeriesKind, "series")

		So(xyGroup.XLogScale, ShouldBeTrue)
		So(AddGroup(ctx, xyGroup), ShouldBeNil)
		So(AddGroup(ctx, seriesGroup), ShouldBeNil)
		So(AddGroup(ctx, NewGroup(XYKind, "xy")), ShouldNotBeNil)
		So(c.Groups, ShouldResemble, []*Group{xyGroup, seriesGroup})

		Convey("Adding graphs", func() {
			Convey("to existing groups", func() {
				pdfGraph, err := EnsureGraph(ctx, XYKind, "p.d.f.", "xy")
				So(err, ShouldBeNil)
				pdfGraph.SetTitle("Distributions").SetXLabel("price").SetYLogScale(true)

				timeGraph, err := EnsureGraph(ctx, SeriesKind, "time", "series")
				So(err, ShouldBeNil)

				So(pdfGraph.Title, ShouldEqual, "Distributions")
				So(pdfGraph.XLabel, ShouldEqual, "price")
				So(pdfGraph.YLogScale, ShouldBeTrue)
				So(pdfGraph.GroupName, ShouldEqual, "xy")

				// Correct graph exists.
				g, err := EnsureGraph(ctx, XYKind, "p.d.f.", "xy")
				So(err, ShouldBeNil)
				So(g, ShouldEqual, pdfGraph)

				_, err = EnsureGraph(ctx, SeriesKind, "wrong", "xy")
				So(err, ShouldNotBeNil)
				// A graph with the wrong kind already exists.
				_, err = EnsureGraph(ctx, XYKind, "time", "xy")
				So(err, ShouldNotBeNil)

				// Duplicate graph name in another group.
				_, err = EnsureGraph(ctx, SeriesKind, "time", "xy")
				So(err, ShouldNotBeNil)

				So(len(c.graphMap), ShouldEqual, 2)
				So(len(c.Groups), ShouldEqual, 2)
				So(c.Groups[0].Graphs[0], ShouldEqual, pdfGraph)
				So(c.Groups[1].Graphs[0], ShouldEqual, timeGraph)
			})

			Convey("to a new group", func() {
				g, err := EnsureGraph(ctx, XYKind, "scatter", "dots")
				So(err, ShouldBeNil)
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

				So(AddGroup(ctx, gr), ShouldBeNil)
				So(c.Groups, ShouldResemble, []*Group{xyGroup, seriesGroup, gr})
				So(len(c.graphMap), ShouldEqual, 2)

				So(c.GetGroup("prep"), ShouldEqual, gr)
				So(c.GetGraph("one"), ShouldEqual, g1)
			})
		})

		Convey("Adding plots", func() {
			g1, err := EnsureGraph(ctx, XYKind, "lines", "xy")
			So(err, ShouldBeNil)
			So(g1.Name, ShouldEqual, "lines")

			_, err = EnsureGraph(ctx, SeriesKind, "prices", "time")
			So(err, ShouldBeNil)

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

			Convey("access methods work", func() {
				x2, y2 := xyPlot.GetXY()
				So(x2, ShouldResemble, x)
				So(y2, ShouldResemble, y)
				So(func() { xyPlot.GetTimeseries() }, ShouldPanic)

				So(func() { timePlot.GetXY() }, ShouldPanic)
				So(timePlot.GetTimeseries(), ShouldResemble, ts)
			})

			Convey("add to the right Y axis", func() {
				So(AddRight(ctx, xyPlot, "lines"), ShouldBeNil)
				So(AddRight(ctx, timePlot, "prices"), ShouldBeNil)
				So(AddRight(ctx, timePlot, "lines"), ShouldNotBeNil)
				So(AddRight(ctx, xyPlot, "nonexistent"), ShouldNotBeNil)

				So(c.graphMap["lines"].PlotsRight, ShouldResemble, []*Plot{xyPlot})
				So(c.graphMap["prices"].PlotsRight, ShouldResemble, []*Plot{timePlot})
			})

			Convey("add to the left Y axis", func() {
				So(AddLeft(ctx, xyPlot, "lines"), ShouldBeNil)
				So(AddLeft(ctx, timePlot, "prices"), ShouldBeNil)
				So(AddLeft(ctx, timePlot, "lines"), ShouldNotBeNil)
				So(AddLeft(ctx, xyPlot, "nonexistent"), ShouldNotBeNil)

				So(c.graphMap["lines"].PlotsLeft, ShouldResemble, []*Plot{xyPlot})
				So(c.graphMap["prices"].PlotsLeft, ShouldResemble, []*Plot{timePlot})
			})
		})
	})
}
