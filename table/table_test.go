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
	"bytes"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

type TestRow struct {
	Make  string
	Model string
}

func (r TestRow) CSV() []string { return []string{r.Make, r.Model} }

func TestBuckets(t *testing.T) {
	t.Parallel()

	Convey("Table methods work", t, func() {
		t := NewTable("Make", "Model")
		headless := NewTable()

		So(t.Header, ShouldResemble, []string{"Make", "Model"})
		t.AddRow(TestRow{"Toyota", "Prius"}, TestRow{"Honda", "Clarity"})
		headless.AddRow(TestRow{"Toyota", "Prius"}, TestRow{"Honda", "Clarity"})

		Convey("AddRow worked", func() {
			So(len(t.Rows), ShouldEqual, 2)
			So(len(headless.Rows), ShouldEqual, 2)
		})

		Convey("WriteCSV", func() {
			Convey("Default Params", func() {
				var buf bytes.Buffer
				So(t.WriteCSV(&buf, Params{}), ShouldBeNil)
				So("\n"+buf.String(), ShouldEqual, `
Make,Model
Toyota,Prius
Honda,Clarity
`)
			})

			Convey("Default Params, headless", func() {
				var buf bytes.Buffer
				So(headless.WriteCSV(&buf, Params{}), ShouldBeNil)
				So("\n"+buf.String(), ShouldEqual, `
Toyota,Prius
Honda,Clarity
`)
			})

			Convey("Limited rows, no header", func() {
				var buf bytes.Buffer
				So(t.WriteCSV(&buf, Params{Rows: 1, NoHeader: true}), ShouldBeNil)
				So("\n"+buf.String(), ShouldEqual, `
Toyota,Prius
`)
			})
		})

		Convey("WriteText", func() {
			Convey("Default Params", func() {
				var buf bytes.Buffer
				So(t.WriteText(&buf, Params{}), ShouldBeNil)
				So("\n"+buf.String(), ShouldEqual, `
  Make |   Model
------ | -------
Toyota |   Prius
 Honda | Clarity
`)
			})

			Convey("Default Params, headless", func() {
				var buf bytes.Buffer
				So(headless.WriteText(&buf, Params{}), ShouldBeNil)
				So("\n"+buf.String(), ShouldEqual, `
Toyota |   Prius
 Honda | Clarity
`)
			})

			Convey("Limited rows and width, no header", func() {
				var buf bytes.Buffer
				So(t.WriteText(&buf, Params{Rows: 1, NoHeader: true, MaxColWidth: 4}), ShouldBeNil)
				So("\n"+buf.String(), ShouldResemble, `
To.. | Pr..
`)
			})
		})
	})
}
