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
	"math"

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
	ChartLine    ChartType = iota
	ChartDashed            // dashed connected line
	ChartScatter           // individual dots
	ChartBars              // histogram bars for each X
	ChartLast              // to check for invalid chart types
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
	case ChartBars:
		return "ChartBars"
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
	X         []float64 `json:"X,omitempty"` // when Kind = KindXY
	Y         []float64
	Dates     []db.Date `json:"Dates,omitempty"` // when Kind = KindSeries
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

// Size returns the number of points in the plot.
func (p *Plot) Size() int {
	return len(p.Y)
}

// MinX computes the smallest value of X. Note, that X might not be sorted
// (e.g. as a scatter plot), so we have to walk through all the values. If
// undefined, returns +Inf.
func (p *Plot) MinX() float64 {
	min := math.Inf(1)
	for _, x := range p.X {
		if x < min {
			min = x
		}
	}
	return min
}

// MaxX computes the largest value of X. Note, that X might not be sorted
// (e.g. as a scatter plot), so we have to walk through all the values. If
// undefined, returns -Inf.
func (p *Plot) MaxX() float64 {
	max := math.Inf(-1)
	for _, x := range p.X {
		if x > max {
			max = x
		}
	}
	return max
}

// MinDate returns the earliest Date for a KindSeries plot, or zero value.
func (p *Plot) MinDate() db.Date {
	if len(p.Dates) == 0 {
		return db.Date{}
	}
	return p.Dates[0]
}

// MaxDate returns the lastest Date for a KindSeries plot, or zero value.
func (p *Plot) MaxDate() db.Date {
	if len(p.Dates) == 0 {
		return db.Date{}
	}
	return p.Dates[len(p.Dates)-1]
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
	Kind       Kind     // each graph can only display one kind of plots
	ID         string   `json:"-"` // unique internal identifier of the graph
	Title      string   // user visible graph title; defaults to Name
	XLabel     string   // label on the X axis
	YLogScale  bool     // whether both Y axis are log-scale
	PlotsRight []*Plot  // plots using the right Y axis
	PlotsLeft  []*Plot  // plots using the left Y axis
	GroupID    string   `json:"-"` // for internal caching in Canvas
	minX       *float64 // exact bounds for all the plots
	maxX       *float64
	minDate    *db.Date
	maxDate    *db.Date
}

func NewGraph(kind Kind, id string) *Graph {
	return &Graph{
		Kind:   kind,
		ID:     id,
		Title:  id,
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

func (g *Graph) updateBounds(p *Plot) {
	if p.Size() == 0 {
		return
	}
	switch g.Kind {
	case KindXY:
		minX := p.MinX()
		maxX := p.MaxX()
		if g.minX == nil {
			g.minX = &minX
		}
		if g.maxX == nil {
			g.maxX = &maxX
		}
		if *g.minX > minX {
			*g.minX = minX
		}
		if *g.maxX < maxX {
			*g.maxX = maxX
		}
	case KindSeries:
		minDate := p.MinDate()
		maxDate := p.MaxDate()
		if g.minDate == nil {
			g.minDate = &minDate
		}
		if g.maxDate == nil {
			g.maxDate = &maxDate
		}
		if g.minDate.After(minDate) {
			*g.minDate = minDate
		}
		if g.maxDate.Before(maxDate) {
			*g.maxDate = maxDate
		}
	}
}

// AddPlotRight adds a plot to be displayed using the right Y axis. It's an
// error if the plot and the Graph have different Kinds.
func (g *Graph) AddPlotRight(p *Plot) error {
	if p.Kind != g.Kind {
		return errors.Reason("plot's kind [%s] != graph's kind [%s]",
			p.Kind, g.Kind)
	}
	g.updateBounds(p)
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
	g.updateBounds(p)
	g.PlotsLeft = append(g.PlotsLeft, p)
	return nil
}

// Group of Graphs sharing the same X axis. Visually, all graphs should be
// displayed one after another in the given order, having the same width and
// aligned vertically, to match their X axis positions.
type Group struct {
	Kind      Kind   // must match Graph's Kind
	ID        string `json:"-"` // internal unique identifier
	Title     string
	XLogScale bool              // whether to use log-scale for X axis
	Graphs    []*Graph          // to preserve the order
	graphMap  map[string]*Graph // for quick access
	MinX      *float64          `json:"MinX,omitempty"`
	MaxX      *float64          `json:"MaxX,omitempty"`
	MinDate   *db.Date          `json:"MinDate,omitempty"`
	MaxDate   *db.Date          `json:"MaxDate,omitempty"`
}

func NewGroup(kind Kind, id string) *Group {
	return &Group{
		Kind:     kind,
		ID:       id,
		Title:    id,
		graphMap: make(map[string]*Graph),
	}
}

func (g *Group) SetTitle(t string) *Group {
	g.Title = t
	return g
}

func (g *Group) SetXLogScale(b bool) *Group {
	g.XLogScale = b
	return g
}

func (g *Group) updateBounds(graph *Graph) {
	switch g.Kind {
	case KindXY:
		if graph.minX != nil {
			minX := *graph.minX
			if g.MinX == nil {
				g.MinX = &minX
			}
			if *g.MinX > minX {
				*g.MinX = minX
			}
		}
		if graph.maxX != nil {
			maxX := *graph.maxX
			if g.MaxX == nil {
				g.MaxX = &maxX
			}
			if *g.MaxX < maxX {
				*g.MaxX = maxX
			}
		}
	case KindSeries:
		if graph.minDate != nil {
			minDate := *graph.minDate
			if g.MinDate == nil {
				g.MinDate = &minDate
			}
			if g.MinDate.After(minDate) {
				*g.MinDate = minDate
			}
		}
		if graph.maxDate != nil {
			maxDate := *graph.maxDate
			if g.MaxDate == nil {
				g.MaxDate = &maxDate
			}
			if g.MaxDate.Before(maxDate) {
				*g.MaxDate = maxDate
			}
		}
	}
}

// addGraph to both the slice and the map, and update graph's GroupID, to keep
// all of them in sync.
func (g *Group) addGraph(graph *Graph) {
	g.Graphs = append(g.Graphs, graph)
	g.graphMap[graph.ID] = graph
	graph.GroupID = g.ID
	g.updateBounds(graph)
}

// AddGraph to the Group. It's an error if the Graph Kind doesn't match the
// Group Kind.
func (g *Group) AddGraph(graph *Graph) error {
	if g.Kind != graph.Kind {
		return errors.Reason("group's Kind [%s] != graph's Kind [%s]",
			g.Kind, graph.Kind)
	}
	if _, ok := g.graphMap[graph.ID]; ok {
		return errors.Reason("graph %s already exists in group %s",
			graph.ID, g.ID)
	}
	g.addGraph(graph)
	return nil
}

// Canvas is the master collection for all the plot groups
type Canvas struct {
	Groups   []*Group          // to preserve the order
	groupMap map[string]*Group // for quick reference by ID
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
	c.groupMap[group.ID] = group
}

func (c *Canvas) AddGroup(group *Group) error {
	if _, ok := c.groupMap[group.ID]; ok {
		return errors.Reason("group %s already exists in Canvas", group.ID)
	}
	// First, just check for graph duplicates, not to modify Canvas in case of an
	// error. We assume that graphs in the group all have different IDs.
	for id := range group.graphMap {
		if _, ok := c.graphMap[id]; ok {
			return errors.Reason("graph %s in group %s already exists in Canvas",
				id, group.ID)
		}
	}
	c.addGroup(group)
	for id, graph := range group.graphMap {
		c.graphMap[id] = graph
	}
	return nil
}

// GetGroup by ID, if it exists, otherwise nil.
func (c *Canvas) GetGroup(id string) *Group {
	g, ok := c.groupMap[id]
	if !ok {
		return nil
	}
	return g
}

// AddGraph to the group by ID. If the group doesn't exist, it is created with
// the same Kind as the Graph.
func (c *Canvas) AddGraph(graph *Graph, groupID string) error {
	if _, ok := c.graphMap[graph.ID]; ok {
		return errors.Reason("graph %s already exists in Canvas", graph.ID)
	}
	group, ok := c.groupMap[groupID]
	if !ok {
		group = NewGroup(graph.Kind, groupID)
		c.addGroup(group)
	}
	if err := group.AddGraph(graph); err != nil {
		return errors.Annotate(err, "failed to add graph %s to group %s",
			graph.ID, groupID)
	}
	c.graphMap[graph.ID] = graph
	return nil
}

// GetGraph by ID, if it exists, otherwise nil.
func (c *Canvas) GetGraph(id string) *Graph {
	g, ok := c.graphMap[id]
	if !ok {
		return nil
	}
	return g
}

func (c *Canvas) updateBounds(graph *Graph) error {
	group, ok := c.groupMap[graph.GroupID]
	if !ok {
		return errors.Reason(
			"graph %s belongs to group %s which doesn't exist in Canvas",
			graph.ID, graph.GroupID)
	}
	group.updateBounds(graph)
	return nil
}

// EnsureGraph creates the requested Graph and/or Group as necessary, and makes
// sure that the existing graph is indeed in the requested group. If the graph
// exists but in the wrong group, it's an error. Returns the graph which can be
// used for further configuration.
func (c *Canvas) EnsureGraph(kind Kind, graphID, groupID string) (*Graph, error) {
	if graph, ok := c.graphMap[graphID]; ok {
		if graph.Kind != kind {
			return nil, errors.Reason("graph %s has kind %s != required kind %s",
				graphID, graph.Kind, kind)
		}
		if graph.GroupID != groupID {
			return nil, errors.Reason(
				"cannot ensure graph %s in group %s: it already exists in group %s",
				graphID, groupID, graph.GroupID)
		}
		return graph, nil
	}
	graph := NewGraph(kind, graphID)
	if err := c.AddGraph(graph, groupID); err != nil {
		return nil, errors.Annotate(err, "cannot ensure graph %s in group %s",
			graphID, groupID)
	}
	return graph, nil
}

// AddPlotRight to the graph by ID to be displayed using the right Y axis. The
// graph must exist in Canvas.
func (c *Canvas) AddPlotRight(p *Plot, graphID string) error {
	graph, ok := c.graphMap[graphID]
	if !ok {
		return errors.Reason("no such graph in Canvas: %s", graphID)
	}
	if err := graph.AddPlotRight(p); err != nil {
		return errors.Annotate(err, "failed to add plot %s to Canvas", p.Legend)
	}
	if err := c.updateBounds(graph); err != nil {
		return errors.Annotate(err, "failed to update bounds")
	}
	return nil
}

// AddPlotLeft to the graph by ID to be displayed using the left Y axis. The
// graph must exist in Canvas.
func (c *Canvas) AddPlotLeft(p *Plot, graphID string) error {
	graph, ok := c.graphMap[graphID]
	if !ok {
		return errors.Reason("no such graph in Canvas: %s", graphID)
	}
	if err := graph.AddPlotLeft(p); err != nil {
		return errors.Annotate(err, "failed to add plot %s to Canvas", p.Legend)
	}
	if err := c.updateBounds(graph); err != nil {
		return errors.Annotate(err, "failed to update bounds")
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
func EnsureGraph(ctx context.Context, kind Kind, graphID, groupID string) (*Graph, error) {
	c := Get(ctx)
	if c == nil {
		return nil, errors.Reason("no Canvas in context")
	}
	return c.EnsureGraph(kind, graphID, groupID)
}

// AddRight adds a plot to the graph by ID for the right Y axis. Canvas must
// exist in the context.
func AddRight(ctx context.Context, p *Plot, graphID string) error {
	c := Get(ctx)
	if c == nil {
		return errors.Reason("no Canvas in context")
	}
	return c.AddPlotRight(p, graphID)
}

// AddLeft adds a plot to the graph by ID for the left Y axis. Canvas must
// exist in the context.
func AddLeft(ctx context.Context, p *Plot, graphID string) error {
	c := Get(ctx)
	if c == nil {
		return errors.Reason("no Canvas in context")
	}
	return c.AddPlotLeft(p, graphID)
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
