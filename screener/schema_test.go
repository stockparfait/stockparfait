// Copyright 2023 Stock Parfait

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//     http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package screener

import (
	"testing"

	"github.com/stockparfait/stockparfait/db"
	"github.com/stockparfait/testutil"

	. "github.com/smartystreets/goconvey/convey"
)

func TestSchema(t *testing.T) {
	t.Parallel()

	Convey("Config works", t, func() {
		confJSON := `
{
  "data": {"DB": "testdb"},
  "columns": [
    {"kind": "ticker", "sort": "descending"},
    {"kind": "price", "date": "2019-01-02"}
  ]
}`
		var config Config
		So(config.InitMessage(testutil.JSON(confJSON)), ShouldBeNil)
		var defaultReader db.Reader
		So(defaultReader.InitMessage(testutil.JSON(`{"DB": "testdb"}`)), ShouldBeNil)
		So(config, ShouldResemble, Config{
			Data: &defaultReader,
			Columns: []Column{
				{Kind: "ticker", Sort: "descending"},
				{Kind: "price", Date: db.NewDate(2019, 1, 2)},
			},
		})
	})
}
