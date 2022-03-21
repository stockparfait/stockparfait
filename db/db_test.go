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
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestDB(t *testing.T) {
	t.Parallel()
	tmpdir, tmpdirErr := ioutil.TempDir("", "testdb")
	defer os.RemoveAll(tmpdir)

	Convey("Test setup succeeded", t, func() {
		So(tmpdirErr, ShouldBeNil)
	})

	Convey("writeGob / readGob work", t, func() {
		Convey("for a slice of struct", func() {
			f := filepath.Join(tmpdir, "slice.gob")
			orig := []TickerRow{TestTicker("A1"), TestTicker("A2")}
			So(writeGob(f, orig), ShouldBeNil)
			var res []TickerRow
			So(readGob(f, &res), ShouldBeNil)
			So(res, ShouldResemble, orig)
		})

		Convey("for a map of struct", func() {
			f := filepath.Join(tmpdir, "slice.gob")
			orig := map[string][]ActionRow{
				"A1": {
					TestAction(NewDate(2019, 1, 1), 1.0, 1.0, true),
					TestAction(NewDate(2020, 1, 1), 1.0, 1.0, false),
				},
				"A2": {
					TestAction(NewDate(2018, 1, 1), 0.99, 1.0, true),
					TestAction(NewDate(2018, 6, 1), 1.0, 0.5, true),
				},
			}
			So(writeGob(f, orig), ShouldBeNil)
			res := make(map[string][]ActionRow)
			So(readGob(f, &res), ShouldBeNil)
			So(res, ShouldResemble, orig)
		})
	})
}
