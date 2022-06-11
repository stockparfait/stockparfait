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
	"fmt"

	"github.com/stockparfait/errors"

	"github.com/stockparfait/stockparfait/db"
	"github.com/stockparfait/stockparfait/stats"
)

// Kind is an enum for different kinds of plots.
type Kind int

// Values of Kind.
const (
	SeriesKind Kind = iota
	XYKind
)

func (k Kind) String() string {
	switch k {
	case SeriesKind:
		return "SeriesKind"
	case XYKind:
		return "XYKind"
	default:
		return fmt.Sprintf("<Undefined Kind: %d>", k)
	}
}

// ChartType is an enum of different ways to plot data.
type ChartType int

// Values of ChartType.
const (
	ChartLine ChartType = iota
	ChartDashed
	ChartScatter
)

// Plot holds data and configuration of a single plot.
type Plot struct {
	Kind      Kind
	X         []float64 // when Kind = XYKind
	Y         []float64
	Dates     []db.Date // when Kind = SeriesKind
	YLabel    string    // value label on the Y axis
	Legend    string    // name in the legend
	ChartType ChartType
}

// NewSeriesPlot creates an instance of a time series plot.
func NewSeriesPlot(ts *stats.Timeseries) *Plot {
	return &Plot{
		Kind:   SeriesKind,
		Y:      ts.Data(),
		Dates:  ts.Dates(),
		YLabel: "values",
		Legend: "Unnamed",
	}
}

// NewXYPlot creates an instance of an untimed plot. Panics if the slices x and
// y don't have the same length.
func NewXYPlot(x, y []float64) *Plot {
	if len(x) != len(y) {
		panic(errors.Reason("len(x)=%d != len(y)=%d", len(x), len(y)))
	}
	return &Plot{
		Kind:   XYKind,
		X:      x,
		Y:      y,
		YLabel: "values",
		Legend: "Unnamed",
	}
}

// SetYLabel of the plot - used as the value label on the Y axis.
func (p *Plot) SetYLabel(label string) *Plot {
	p.YLabel = label
	return p
}

// SetLegend of the plot - used as the plot's name in the legend.
func (p *Plot) SetLegend(legend string) *Plot {
	p.Legend = legend
	return p
}

// SetChartType - how to plot the data.
func (p *Plot) SetChartType(t ChartType) *Plot {
	p.ChartType = t
	return p
}

// GetTimeseries from a series Plot. Panics if Kind != SeriesKind.
func (p *Plot) GetTimeseries() *stats.Timeseries {
	if p.Kind != SeriesKind {
		panic(errors.Reason("Kind is not SeriesKind"))
	}
	return stats.NewTimeseries().Init(p.Dates, p.Y)
}

// GetXY extracts X and Y slices from an untimed Plot. Panics if Kind != XYKind.
func (p *Plot) GetXY() ([]float64, []float64) {
	if p.Kind != XYKind {
		panic(errors.Reason("Kind is not XYKind"))
	}
	return p.X, p.Y
}

// Graph containing Plots.
type Graph struct {
	Kind       Kind
	Name       string
	Title      string
	XLabel     string
	YLogScale  bool
	PlotsRight []*Plot // plots using the right Y axis
	PlotsLeft  []*Plot // plots using the left Y axis
}

func NewGraph(kind Kind, name string) *Graph {
	return &Graph{
		Kind:   kind,
		Name:   name,
		Title:  "Unnamed",
		XLabel: "Value",
	}
}

func (g *Graph) SetTitle(t string) *Graph {
	g.Title = t
	return g
}

func (g *Graph) SetXLabel(l string) *Graph {
	g.XLabel = l
	return g
}

func (g *Graph) SetYLogScale(b bool) *Graph {
	g.YLogScale = b
	return g
}

// AddPlotRight adds a plot to be displayed using the right Y axis. It's an
// error if the plot and the Graph have different Kinds.
func (g *Graph) AddPlotRight(p *Plot) error {
	if p.Kind != g.Kind {
		return errors.Reason("plot's kind [%s] != graph's kind [%s]",
			p.Kind, g.Kind)
	}
	g.PlotsRight = append(g.PlotsRight, p)
	return nil
}

// AddPlotLeft adds a plot to be displayed using the left Y axis. It's an error
// if the plot and the Graph have different Kinds.
func (g *Graph) AddPlotLeft(p *Plot) error {
	if p.Kind != g.Kind {
		return errors.Reason("plot's kind [%s] != graph's kind [%s]",
			p.Kind, g.Kind)
	}
	g.PlotsLeft = append(g.PlotsLeft, p)
	return nil
}

// Group of Graphs sharding the same X axis.
type Group struct {
	Kind      Kind // must match Graph's Kind
	Name      string
	XLogScale bool
	Graphs    map[string]*Graph
}

func NewGroup(kind Kind, name string) *Group {
	return &Group{
		Kind:   kind,
		Name:   name,
		Graphs: make(map[string]*Graph),
	}
}

func (g *Group) SetXLogScale(b bool) *Group {
	g.XLogScale = b
	return g
}

// AddGraph to the Group. It's an error if the Graph Kind doesn't match the
// Group Kind.
func (g *Group) AddGraph(graph *Graph) error {
	if g.Kind != graph.Kind {
		return errors.Reason("group's Kind [%s] != graph's Kind [%s]",
			g.Kind, graph.Kind)
	}
	if _, ok := g.Graphs[graph.Name]; ok {
		return errors.Reason("graph %s already exists in group %s",
			graph.Name, g.Name)
	}
	g.Graphs[graph.Name] = graph
	return nil
}

// Canvas is the master collection for all the plot groups
type Canvas struct {
	Groups map[string]*Group
	Graphs map[string]*Graph // direct Graph reference within Groups
}

func NewCanvas() *Canvas {
	return &Canvas{
		Groups: make(map[string]*Group),
		Graphs: make(map[string]*Graph),
	}
}

func (c *Canvas) AddGroup(group *Group) error {
	if _, ok := c.Groups[group.Name]; ok {
		return errors.Reason("group %s already exists in Canvas", group.Name)
	}
	c.Groups[group.Name] = group
	for name, graph := range group.Graphs {
		if _, ok := c.Graphs[name]; ok {
			return errors.Reason("graph %s in group %s already exists in Canvas",
				name, group.Name)
		}
		c.Graphs[name] = graph
	}
	return nil
}

// AddGraph to the group by name. If the group doesn't exist, it is created with
// the same Kind as the Graph.
func (c *Canvas) AddGraph(graph *Graph, groupName string) error {
	if _, ok := c.Graphs[graph.Name]; ok {
		return errors.Reason("graph %s already exists in Canvas", graph.Name)
	}
	group, ok := c.Groups[groupName]
	if !ok {
		group = NewGroup(graph.Kind, groupName)
		c.Groups[groupName] = group
	}
	if err := group.AddGraph(graph); err != nil {
		return errors.Annotate(err, "failed to add graph %s to group %s",
			graph.Name, groupName)
	}
	c.Graphs[graph.Name] = graph
	return nil
}

// AddPlotRight to the graph by name to be displayed using the right Y axis. The
// graph must exist in Canvas.
func (c *Canvas) AddPlotRight(p *Plot, graphName string) error {
	graph, ok := c.Graphs[graphName]
	if !ok {
		return errors.Reason("no such graph in Canvas: %s", graphName)
	}
	if err := graph.AddPlotRight(p); err != nil {
		return errors.Annotate(err, "failed to add plot %s to Canvas", p.Legend)
	}
	return nil
}

// AddPlotLeft to the graph by name to be displayed using the left Y axis. The
// graph must exist in Canvas.
func (c *Canvas) AddPlotLeft(p *Plot, graphName string) error {
	graph, ok := c.Graphs[graphName]
	if !ok {
		return errors.Reason("no such graph in Canvas: %s", graphName)
	}
	if err := graph.AddPlotLeft(p); err != nil {
		return errors.Annotate(err, "failed to add plot %s to Canvas", p.Legend)
	}
	return nil
}
