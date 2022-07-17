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
	"bytes"
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
		xyGroup := NewGroup(KindXY, "xy").SetXLogScale(true)
		seriesGroup := NewGroup(KindSeries, "series")

		So(xyGroup.XLogScale, ShouldBeTrue)
		So(AddGroup(ctx, xyGroup), ShouldBeNil)
		So(AddGroup(ctx, seriesGroup), ShouldBeNil)
		So(AddGroup(ctx, NewGroup(KindXY, "xy")), ShouldNotBeNil)
		So(c.Groups, ShouldResemble, []*Group{xyGroup, seriesGroup})

		Convey("Adding graphs", func() {
			Convey("to existing groups", func() {
				pdfGraph, err := EnsureGraph(ctx, KindXY, "p.d.f.", "xy")
				So(err, ShouldBeNil)
				pdfGraph.SetTitle("Distributions").SetXLabel("price").SetYLogScale(true)

				timeGraph, err := EnsureGraph(ctx, KindSeries, "time", "series")
				So(err, ShouldBeNil)

				So(pdfGraph.Title, ShouldEqual, "Distributions")
				So(pdfGraph.XLabel, ShouldEqual, "price")
				So(pdfGraph.YLogScale, ShouldBeTrue)
				So(pdfGraph.GroupID, ShouldEqual, "xy")

				// Correct graph exists.
				g, err := EnsureGraph(ctx, KindXY, "p.d.f.", "xy")
				So(err, ShouldBeNil)
				So(g, ShouldEqual, pdfGraph)

				_, err = EnsureGraph(ctx, KindSeries, "wrong", "xy")
				So(err, ShouldNotBeNil)
				// A graph with the wrong kind already exists.
				_, err = EnsureGraph(ctx, KindXY, "time", "xy")
				So(err, ShouldNotBeNil)

				// Duplicate graph ID in another group.
				_, err = EnsureGraph(ctx, KindSeries, "time", "xy")
				So(err, ShouldNotBeNil)

				So(len(c.graphMap), ShouldEqual, 2)
				So(len(c.Groups), ShouldEqual, 2)
				So(c.Groups[0].Graphs[0], ShouldEqual, pdfGraph)
				So(c.Groups[1].Graphs[0], ShouldEqual, timeGraph)
			})

			Convey("to a new group", func() {
				g, err := EnsureGraph(ctx, KindXY, "scatter", "dots")
				So(err, ShouldBeNil)
				So(len(c.Groups), ShouldEqual, 3)
				So(c.Groups[:2], ShouldResemble, []*Group{xyGroup, seriesGroup})
				So(c.Groups[2].Graphs[0], ShouldEqual, g)
			})

			Convey("from a group pre-populated with graphs", func() {
				gr := NewGroup(KindXY, "prep")
				g1 := NewGraph(KindXY, "one")
				g2 := NewGraph(KindXY, "two")

				So(gr.AddGraph(g1), ShouldBeNil)
				So(gr.AddGraph(g2), ShouldBeNil)
				So(gr.AddGraph(NewGraph(KindXY, "one")), ShouldNotBeNil)

				So(AddGroup(ctx, gr), ShouldBeNil)
				So(c.Groups, ShouldResemble, []*Group{xyGroup, seriesGroup, gr})
				So(len(c.graphMap), ShouldEqual, 2)

				So(c.GetGroup("prep"), ShouldEqual, gr)
				So(c.GetGraph("one"), ShouldEqual, g1)
			})
		})

		Convey("Adding plots", func() {
			g1, err := EnsureGraph(ctx, KindXY, "lines", "xy")
			So(err, ShouldBeNil)
			So(g1.ID, ShouldEqual, "lines")

			_, err = EnsureGraph(ctx, KindSeries, "prices", "time")
			So(err, ShouldBeNil)

			x1 := []float64{1.0, 2.0, 3.0}
			y1 := []float64{10.0, 20.0, 30.0}
			dates1 := []db.Date{
				db.NewDate(2020, 1, 1),
				db.NewDate(2020, 1, 2),
				db.NewDate(2020, 1, 3),
			}
			ts1 := stats.NewTimeseries().Init(dates1, y1)

			x2 := []float64{-1.5, 1.0, 3.5}
			y2 := []float64{20.0, 30.0, 10.0}
			dates2 := []db.Date{
				db.NewDate(2019, 12, 31),
				db.NewDate(2020, 1, 2),
				db.NewDate(2020, 3, 1),
			}
			ts2 := stats.NewTimeseries().Init(dates2, y2)

			xyPlot1 := NewXYPlot(x1, y1).
				SetYLabel("p").
				SetLegend("PDF").
				SetChartType(ChartDashed)

			timePlot1 := NewSeriesPlot(ts1)

			xyPlot2 := NewXYPlot(x2, y2).SetChartType(ChartScatter)
			timePlot2 := NewSeriesPlot(ts2)

			Convey("access methods work", func() {
				xx, yy := xyPlot1.GetXY()
				So(xx, ShouldResemble, x1)
				So(yy, ShouldResemble, y1)
				So(func() { xyPlot1.GetTimeseries() }, ShouldPanic)
				So(xyPlot1.MinX(), ShouldEqual, 1.0)
				So(xyPlot1.MaxX(), ShouldEqual, 3.0)

				So(func() { timePlot1.GetXY() }, ShouldPanic)
				So(timePlot1.GetTimeseries(), ShouldResemble, ts1)
				So(timePlot1.MinDate(), ShouldResemble, db.NewDate(2020, 1, 1))
				So(timePlot1.MaxDate(), ShouldResemble, db.NewDate(2020, 1, 3))
			})

			Convey("add to the right Y axis", func() {
				So(AddRight(ctx, xyPlot1, "lines"), ShouldBeNil)
				So(AddRight(ctx, timePlot1, "prices"), ShouldBeNil)
				So(AddRight(ctx, timePlot1, "lines"), ShouldNotBeNil)
				So(AddRight(ctx, xyPlot1, "nonexistent"), ShouldNotBeNil)

				So(AddRight(ctx, xyPlot2, "lines"), ShouldBeNil)
				So(AddRight(ctx, timePlot2, "prices"), ShouldBeNil)

				So(c.graphMap["lines"].PlotsRight, ShouldResemble, []*Plot{
					xyPlot1, xyPlot2})
				So(c.graphMap["lines"].Kind, ShouldEqual, KindXY)
				So(*c.graphMap["lines"].minX, ShouldEqual, -1.5)
				So(*c.graphMap["lines"].maxX, ShouldEqual, 3.5)

				So(c.graphMap["prices"].PlotsRight, ShouldResemble, []*Plot{
					timePlot1, timePlot2})
				So(c.graphMap["prices"].Kind, ShouldEqual, KindSeries)
				So(*c.graphMap["prices"].minDate, ShouldResemble, db.NewDate(2019, 12, 31))
				So(*c.graphMap["prices"].maxDate, ShouldResemble, db.NewDate(2020, 3, 1))

				So(*c.groupMap["xy"].MinX, ShouldEqual, -1.5)
				So(*c.groupMap["xy"].MaxX, ShouldEqual, 3.5)
			})

			Convey("add to the left Y axis", func() {
				So(AddLeft(ctx, xyPlot1, "lines"), ShouldBeNil)
				So(AddLeft(ctx, timePlot1, "prices"), ShouldBeNil)
				So(AddLeft(ctx, timePlot1, "lines"), ShouldNotBeNil)
				So(AddLeft(ctx, xyPlot1, "nonexistent"), ShouldNotBeNil)

				So(c.graphMap["lines"].PlotsLeft, ShouldResemble, []*Plot{xyPlot1})
				So(c.graphMap["prices"].PlotsLeft, ShouldResemble, []*Plot{timePlot1})
			})

			Convey("JSON conversion works", func() {
				xyPlotBars := NewXYPlot(x2, y2).SetChartType(ChartBars)

				So(AddLeft(ctx, xyPlot1, "lines"), ShouldBeNil)
				So(AddLeft(ctx, timePlot1, "prices"), ShouldBeNil)
				So(AddRight(ctx, xyPlot2, "lines"), ShouldBeNil)
				So(AddRight(ctx, xyPlotBars, "lines"), ShouldBeNil)
				So(AddRight(ctx, timePlot2, "prices"), ShouldBeNil)
				var buf bytes.Buffer
				So(WriteJS(ctx, &buf), ShouldBeNil)
				So("\n"+buf.String(), ShouldEqual, `
var DATA = {"Groups":[{"Kind":"KindXY","Title":"xy","XLogScale":true,"Graphs":[{"Kind":"KindXY","Title":"lines","XLabel":"Value","YLogScale":false,"PlotsRight":[{"Kind":"KindXY","X":[-1.5,1,3.5],"Y":[20,30,10],"YLabel":"values","Legend":"Unnamed","ChartType":"ChartScatter"},{"Kind":"KindXY","X":[-1.5,1,3.5],"Y":[20,30,10],"YLabel":"values","Legend":"Unnamed","ChartType":"ChartBars"}],"PlotsLeft":[{"Kind":"KindXY","X":[1,2,3],"Y":[10,20,30],"YLabel":"p","Legend":"PDF","ChartType":"ChartDashed"}]}],"MinX":-1.5,"MaxX":3.5},{"Kind":"KindSeries","Title":"series","XLogScale":false,"Graphs":null},{"Kind":"KindSeries","Title":"time","XLogScale":false,"Graphs":[{"Kind":"KindSeries","Title":"prices","XLabel":"Value","YLogScale":false,"PlotsRight":[{"Kind":"KindSeries","Y":[20,30,10],"Dates":["2019-12-31","2020-01-02","2020-03-01"],"YLabel":"values","Legend":"Unnamed","ChartType":"ChartLine"}],"PlotsLeft":[{"Kind":"KindSeries","Y":[10,20,30],"Dates":["2020-01-01","2020-01-02","2020-01-03"],"YLabel":"values","Legend":"Unnamed","ChartType":"ChartLine"}]}],"MinDate":"2019-12-31","MaxDate":"2020-03-01"}]}
;`)
			})
		})
	})
}
