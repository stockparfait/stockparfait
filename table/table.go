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

package table

import (
	"encoding/csv"
	"fmt"
	"io"
	"strings"

	"github.com/stockparfait/errors"
)

// Row interface that a table row representation must implement.
type Row interface {
	CSV() []string // an encoding/csv compatible row representation
}

// Table container.
//
// A typical use:
//   type MyRow struct {
//     Name string
//     Age int
//   }
//
//   func (r MyRow) CSV() []string {
//     return []string{r.Name, fmt.Sprintf("%d", r.Age)}
//   }
//   t := NewTable([]string{"Name", "Age"})
//   t.AddRow(MyRow{"John", 25}, MyRow{"Jane", 24})
type Table struct {
	Header []string // optional, may be nil
	Rows   []Row
}

// NewTable creates a new Table instance with optional column headers.  It is
// expected that, when present, the number of column headers is the same as the
// number of elements in each Row.
func NewTable(header ...string) *Table {
	return &Table{Header: header}
}

// AddRow adds one or more rows to the table.
func (t *Table) AddRow(rows ...Row) {
	t.Rows = append(t.Rows, rows...)
}

// Params are parameters for pretty-printing or CSV export of Table data.
type Params struct {
	Rows        int  // max. number of rows to write; 0 = unlimited (default)
	NoHeader    bool // whether to print the header, default - yes
	MaxColWidth int  // for WriteText only; 0 = unlimited, otherwise must be >= 4
}

// WriteCSV writes the entire table to w in CSV format.
func (t *Table) WriteCSV(w io.Writer, p Params) error {
	cw := csv.NewWriter(w)
	if !p.NoHeader && len(t.Header) > 0 {
		if err := cw.Write(t.Header); err != nil {
			return errors.Annotate(err, "failed to write header")
		}
	}
	for i, r := range t.Rows {
		if p.Rows > 0 && i >= p.Rows {
			break
		}
		if err := cw.Write(r.CSV()); err != nil {
			return errors.Annotate(err, "failed to write row")
		}
	}
	cw.Flush()
	if err := cw.Error(); err != nil {
		return errors.Annotate(err, "failed to flush written rows")
	}
	return nil
}

// WriteText writes the table as a text formatted for ease of reading.
func (t *Table) WriteText(w io.Writer, p Params) error {
	if p.MaxColWidth != 0 && p.MaxColWidth < 4 {
		return errors.Reason("MaxColWidth [%d] must be 0 or >= 4", p.MaxColWidth)
	}
	var widths []int
	update := func(row []string) error {
		if len(row) == 0 {
			return errors.Reason("row size = 0")
		}
		if len(widths) == 0 {
			widths = make([]int, len(row))
		}
		if len(row) != len(widths) {
			return errors.Reason("row size [%d] != expected size [%d]",
				len(row), len(widths))
		}
		for i := range widths {
			if widths[i] < len(row[i]) {
				widths[i] = len(row[i])
				if p.MaxColWidth > 0 && widths[i] > p.MaxColWidth {
					widths[i] = p.MaxColWidth
				}
			}
		}
		return nil
	}

	write := func(row []string) error {
		trimmed := make([]string, len(row))
		for i, s := range row {
			trimmed[i] = s
			if len([]rune(s)) > widths[i] {
				r := []rune(s)[:widths[i]-2]
				trimmed[i] = string(r) + ".."
			}
			trimmed[i] = fmt.Sprintf("%[2]*[1]s", trimmed[i], widths[i])
		}
		_, err := fmt.Fprintf(w, "%s\n", strings.Join(trimmed, " | "))
		return err
	}

	dashes := func(n int) string {
		b := make([]byte, n)
		for i := range b {
			b[i] = byte('-')
		}
		return string(b)
	}

	dashedRow := func() []string {
		row := make([]string, len(widths))
		for i, w := range widths {
			row[i] = dashes(w)
		}
		return row
	}

	if !p.NoHeader && len(t.Header) > 0 {
		if err := update(t.Header); err != nil {
			return errors.Annotate(err, "failed to update header widths")
		}
	}
	for i, r := range t.Rows {
		if p.Rows > 0 && i >= p.Rows {
			break
		}
		if err := update(r.CSV()); err != nil {
			return errors.Annotate(err, "failed to update row widths")
		}
	}

	if !p.NoHeader && len(t.Header) > 0 {
		if err := write(t.Header); err != nil {
			return errors.Annotate(err, "failed to write header")
		}
		if err := write(dashedRow()); err != nil {
			return errors.Annotate(err, "failed to write header separator")
		}
	}
	for i, r := range t.Rows {
		if p.Rows > 0 && i >= p.Rows {
			break
		}
		if err := write(r.CSV()); err != nil {
			return errors.Annotate(err, "failed to write row")
		}
	}
	return nil
}
