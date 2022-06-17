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
	"encoding/json"
	"fmt"
	"io"

	"github.com/stockparfait/errors"

	"github.com/stockparfait/stockparfait/db"
	"github.com/stockparfait/stockparfait/stats"
)

// Kind is an enum for different kinds of plots; currently time series and
// arbitrary (x, y) plots, such as curves or scatter plots.
type Kind int

// Values of Kind.
const (
	KindSeries Kind = iota
	KindXY
	KindLast // to check for invalid kinds
)

var _ json.Marshaler = KindSeries

func (k Kind) String() string {
	switch k {
	case KindSeries:
		return "KindSeries"
	case KindXY:
		return "KindXY"
	default:
		return fmt.Sprintf("<Undefined Kind: %d>", k)
	}
}

// MarshalJSON implements json.Marshaler.
func (k Kind) MarshalJSON() ([]byte, error) {
	if k >= KindLast {
		return nil, errors.Reason("invalid kind: %s", k)
	}
	return []byte(`"` + k.String() + `"`), nil
}

// ChartType is an enum of different ways to plot data: as a connected solid or
// dashed line, or as individual dots for scatter plots.
type ChartType int

// Values of ChartType.
const (
	ChartLine ChartType = iota
	ChartDashed
	ChartScatter
	ChartLast // to check for invalid chart types
)

var _ json.Marshaler = ChartLine

func (c ChartType) String() string {
	switch c {
	case ChartLine:
		return "ChartLine"
	case ChartDashed:
		return "ChartDashed"
	case ChartScatter:
		return "ChartScatter"
	default:
		return fmt.Sprintf("<Undefined ChartType: %d>", c)
	}
}

// MarshalJSON implements json.Marshaler.
func (c ChartType) MarshalJSON() ([]byte, error) {
	if c >= ChartLast {
		return nil, errors.Reason("invalid chart type: %s", c)
	}
	return []byte(`"` + c.String() + `"`), nil
}

// Plot holds data and configuration of a single plot.
type Plot struct {
	Kind      Kind
	X         []float64 // when Kind = KindXY
	Y         []float64
	Dates     []db.Date // when Kind = KindSeries
	YLabel    string    // value label on the Y axis
	Legend    string    // name in the legend
	ChartType ChartType
}

// NewSeriesPlot creates an instance of a time series plot.
func NewSeriesPlot(ts *stats.Timeseries) *Plot {
	return &Plot{
		Kind:   KindSeries,
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
		Kind:   KindXY,
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

// GetTimeseries from a series Plot. Panics if Kind != KindSeries.
func (p *Plot) GetTimeseries() *stats.Timeseries {
	if p.Kind != KindSeries {
		panic(errors.Reason("Kind is not KindSeries"))
	}
	return stats.NewTimeseries().Init(p.Dates, p.Y)
}

// GetXY extracts X and Y slices from an untimed Plot. Panics if Kind != KindXY.
func (p *Plot) GetXY() ([]float64, []float64) {
	if p.Kind != KindXY {
		panic(errors.Reason("kind %s is not KindXY", p.Kind))
	}
	return p.X, p.Y
}

// Graph is a container for Plots, and visually it corresponds to a single (X,
// Y) or (Date, Y) chart where these plots are displayed.
type Graph struct {
	Kind       Kind    // each graph can only display one kind of plots
	Name       string  `json:"-"` // unique internal name of the graph
	Title      string  // user visible graph title; defaults to Name
	XLabel     string  // label on the X axis
	YLogScale  bool    // whether both Y axis are log-scale
	PlotsRight []*Plot // plots using the right Y axis
	PlotsLeft  []*Plot // plots using the left Y axis
	GroupName  string  `json:"-"` // for internal caching in Canvas
}

func NewGraph(kind Kind, name string) *Graph {
	return &Graph{
		Kind:   kind,
		Name:   name,
		Title:  name,
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

// Group of Graphs sharing the same X axis. Visually, all graphs should be
// displayed one after another in the given order, having the same width and
// aligned vertically, to match their X axis positions.
type Group struct {
	Kind      Kind              // must match Graph's Kind
	Name      string            `json:"-"` // internal unique name of the group
	XLogScale bool              // whether to use log-scale for X axis
	Graphs    []*Graph          // to preserve the order
	graphMap  map[string]*Graph // for quick access
}

func NewGroup(kind Kind, name string) *Group {
	return &Group{
		Kind:     kind,
		Name:     name,
		graphMap: make(map[string]*Graph),
	}
}

func (g *Group) SetXLogScale(b bool) *Group {
	g.XLogScale = b
	return g
}

// addGraph to both the slice and the map, and update graph's GroupName, to keep
// all of them in sync.
func (g *Group) addGraph(graph *Graph) {
	g.Graphs = append(g.Graphs, graph)
	g.graphMap[graph.Name] = graph
	graph.GroupName = g.Name
}

// AddGraph to the Group. It's an error if the Graph Kind doesn't match the
// Group Kind.
func (g *Group) AddGraph(graph *Graph) error {
	if g.Kind != graph.Kind {
		return errors.Reason("group's Kind [%s] != graph's Kind [%s]",
			g.Kind, graph.Kind)
	}
	if _, ok := g.graphMap[graph.Name]; ok {
		return errors.Reason("graph %s already exists in group %s",
			graph.Name, g.Name)
	}
	g.addGraph(graph)
	return nil
}

// Canvas is the master collection for all the plot groups
type Canvas struct {
	Groups   []*Group          // to preserve the order
	groupMap map[string]*Group // for quick reference by name
	graphMap map[string]*Graph // direct Graph reference within Groups
}

func NewCanvas() *Canvas {
	return &Canvas{
		groupMap: make(map[string]*Group),
		graphMap: make(map[string]*Graph),
	}
}

// addGroup to both the slice and the map, to keep them in sync.
func (c *Canvas) addGroup(group *Group) {
	c.Groups = append(c.Groups, group)
	c.groupMap[group.Name] = group
}

func (c *Canvas) AddGroup(group *Group) error {
	if _, ok := c.groupMap[group.Name]; ok {
		return errors.Reason("group %s already exists in Canvas", group.Name)
	}
	// First, just check for graph duplicates, not to modify Canvas in case of an
	// error. We assume that graphs in the group all have different names.
	for name := range group.graphMap {
		if _, ok := c.graphMap[name]; ok {
			return errors.Reason("graph %s in group %s already exists in Canvas",
				name, group.Name)
		}
	}
	c.addGroup(group)
	for name, graph := range group.graphMap {
		c.graphMap[name] = graph
	}
	return nil
}

// GetGroup by name, if it exists, otherwise nil.
func (c *Canvas) GetGroup(name string) *Group {
	g, ok := c.groupMap[name]
	if !ok {
		return nil
	}
	return g
}

// AddGraph to the group by name. If the group doesn't exist, it is created with
// the same Kind as the Graph.
func (c *Canvas) AddGraph(graph *Graph, groupName string) error {
	if _, ok := c.graphMap[graph.Name]; ok {
		return errors.Reason("graph %s already exists in Canvas", graph.Name)
	}
	group, ok := c.groupMap[groupName]
	if !ok {
		group = NewGroup(graph.Kind, groupName)
		c.addGroup(group)
	}
	if err := group.AddGraph(graph); err != nil {
		return errors.Annotate(err, "failed to add graph %s to group %s",
			graph.Name, groupName)
	}
	c.graphMap[graph.Name] = graph
	return nil
}

// GetGraph by name, if it exists, otherwise nil.
func (c *Canvas) GetGraph(name string) *Graph {
	g, ok := c.graphMap[name]
	if !ok {
		return nil
	}
	return g
}

// EnsureGraph creates the requested Graph and/or Group as necessary, and makes
// sure that the existing graph is indeed in the requested group. If the graph
// exists but in the wrong group, it's an error. Returns the graph which can be
// used for further configuration.
func (c *Canvas) EnsureGraph(kind Kind, graphName, groupName string) (*Graph, error) {
	if graph, ok := c.graphMap[graphName]; ok {
		if graph.Kind != kind {
			return nil, errors.Reason("graph %s has kind %s != required kind %s",
				graphName, graph.Kind, kind)
		}
		if graph.GroupName != groupName {
			return nil, errors.Reason(
				"cannot ensure graph %s in group %s: it already exists in group %s",
				graphName, groupName, graph.GroupName)
		}
		return graph, nil
	}
	graph := NewGraph(kind, graphName)
	if err := c.AddGraph(graph, groupName); err != nil {
		return nil, errors.Annotate(err, "cannot ensure graph %s in group %s",
			graphName, groupName)
	}
	return graph, nil
}

// AddPlotRight to the graph by name to be displayed using the right Y axis. The
// graph must exist in Canvas.
func (c *Canvas) AddPlotRight(p *Plot, graphName string) error {
	graph, ok := c.graphMap[graphName]
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
	graph, ok := c.graphMap[graphName]
	if !ok {
		return errors.Reason("no such graph in Canvas: %s", graphName)
	}
	if err := graph.AddPlotLeft(p); err != nil {
		return errors.Annotate(err, "failed to add plot %s to Canvas", p.Legend)
	}
	return nil
}

func (c *Canvas) WriteJSON(w io.Writer) error {
	enc := json.NewEncoder(w)
	if err := enc.Encode(c); err != nil {
		return errors.Annotate(err, "failed to encode JSON")
	}
	return nil
}

// WriteJS writes "var DATA = <JSON>;" string to w, suitable for importing as a
// javascript module.
func (c *Canvas) WriteJS(w io.Writer) error {
	_, err := fmt.Fprintf(w, "var DATA = ")
	if err != nil {
		return errors.Annotate(err, "failed to write JS prefix")
	}
	if err = c.WriteJSON(w); err != nil {
		return errors.Annotate(err, "failed to write JSON part of JS")
	}
	_, err = fmt.Fprintf(w, ";")
	if err != nil {
		return errors.Annotate(err, "failed to write JS suffix")
	}
	return nil
}

type contextKey int

const (
	canvasContextKey contextKey = iota
)

// Use injects the Canvas into the context.
func Use(ctx context.Context, c *Canvas) context.Context {
	return context.WithValue(ctx, canvasContextKey, c)
}

// Get a Canvas instance from the context, or nil if not present.
func Get(ctx context.Context) *Canvas {
	c, ok := ctx.Value(canvasContextKey).(*Canvas)
	if !ok {
		return nil
	}
	return c
}

// AddGroup to the Canvas in context. It's an error if Canvas is not in context.
func AddGroup(ctx context.Context, group *Group) error {
	c := Get(ctx)
	if c == nil {
		return errors.Reason("no Canvas in context")
	}
	return c.AddGroup(group)
}

// EnsureGraph in the Canvas in context. It's an error if Canvas is not in
// context.
func EnsureGraph(ctx context.Context, kind Kind, graphName, groupName string) (*Graph, error) {
	c := Get(ctx)
	if c == nil {
		return nil, errors.Reason("no Canvas in context")
	}
	return c.EnsureGraph(kind, graphName, groupName)
}

// AddRight adds a plot to the graph by name for the right Y axis. Canvas must
// exist in the context.
func AddRight(ctx context.Context, p *Plot, graphName string) error {
	c := Get(ctx)
	if c == nil {
		return errors.Reason("no Canvas in context")
	}
	return c.AddPlotRight(p, graphName)
}

// AddLeft adds a plot to the graph by name for the left Y axis. Canvas must
// exist in the context.
func AddLeft(ctx context.Context, p *Plot, graphName string) error {
	c := Get(ctx)
	if c == nil {
		return errors.Reason("no Canvas in context")
	}
	return c.AddPlotLeft(p, graphName)
}

func WriteJSON(ctx context.Context, w io.Writer) error {
	c := Get(ctx)
	if c == nil {
		return errors.Reason("no Canvas in context")
	}
	return c.WriteJSON(w)
}

func WriteJS(ctx context.Context, w io.Writer) error {
	c := Get(ctx)
	if c == nil {
		return errors.Reason("no Canvas in context")
	}
	return c.WriteJS(w)
}
