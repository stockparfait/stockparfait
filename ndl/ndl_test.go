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

package ndl

import (
	"context"
	"fmt"
	"net/url"
	"testing"

	"github.com/stockparfait/fetch"
	"github.com/stockparfait/stockparfait/db"

	. "github.com/smartystreets/goconvey/convey"
)

var testSchema = Schema{
	{Name: "num", Type: "Integer"},
	{Name: "str", Type: "String"},
}

type testRow struct {
	Num int
	Str string
}

var _ ValueLoader = &testRow{}

func (t *testRow) Load(v []Value, s Schema) error {
	if !testSchema.Equal(s) {
		return fmt.Errorf("testRow.Load: unexpected schema: %v", s)
	}
	if len(v) != len(testSchema) {
		return fmt.Errorf("testRow.Load: expected %d values, received %d: %v",
			len(testSchema), len(v), v)
	}
	var ok bool
	// any number in JSON is unmarshaled as float64
	var num float64
	if num, ok = v[0].(float64); !ok {
		return fmt.Errorf("testRow.Load: v[0] = %v is of the wrong type: %T", v[0], v[0])
	}
	t.Num = int(num)
	if t.Str, ok = v[1].(string); !ok {
		return fmt.Errorf("testRow.Load: v[1] = %v is of the wrong type: %T", v[1], v[1])
	}
	return nil
}

func rowsAll(it *RowIterator) ([]*testRow, error) {
	rows := []*testRow{}
	for {
		row := testRow{}
		ok, err := it.Next(&row)
		if !ok {
			break
		}
		if err != nil {
			return rows, err
		}
		rows = append(rows, &row)
		if len(rows) > 1000 {
			return nil, fmt.Errorf("rowsAll: too many rows - %d", len(rows))
		}
	}
	return rows, nil
}

func TestNDL(t *testing.T) {
	t.Parallel()

	Convey("TableQuery builds nondestructively", t, func() {
		Convey("Equality", func() {
			q := NewTableQuery("test/table")
			q2 := q.Equal("col", "v1", "v2")
			So(len(q.Values()), ShouldEqual, 0)
			So(q2.Values(), ShouldResemble, url.Values{"col": []string{"v1,v2"}})
		})

		Convey("Comparisons", func() {
			q := NewTableQuery("test/table")
			q2 := q.Lt("col", "5")
			So(len(q.Values()), ShouldEqual, 0)
			So(q2.Values(), ShouldResemble, url.Values{"col.lt": []string{"5"}})
		})

		Convey("Options", func() {
			q := NewTableQuery("test/table")
			q2 := q.Columns("c1", "c2")
			q4 := q.PerPage(100)
			q5 := q.Cursor("blah")
			So(len(q.Values()), ShouldEqual, 0)
			So(q2.Values(), ShouldResemble, url.Values{"qopts.columns": []string{"c1,c2"}})
			So(q4.Values(), ShouldResemble, url.Values{"qopts.per_page": []string{"100"}})
			So(q5.Values(), ShouldResemble, url.Values{"qopts.cursor_id": []string{"blah"}})
		})
	})

	Convey("API calls work correctly", t, func() {
		server := fetch.NewTestServer()
		defer server.Close()
		server.ResponseBody = []string{"{}"}

		testKey := "testkey"
		ctx := fetch.UseClient(context.Background(), server.Client())
		URL = server.URL() + "/api/v3"
		ctx = UseClient(ctx, testKey)

		Convey("TableQuery", func() {
			Convey("fetches one page", func() {
				expected := []*testRow{{42, "one"}, {84, "two"}}
				page, err := TestTablePage([][]Value{{42, "one"}, {84, "two"}}, testSchema, "")
				So(err, ShouldBeNil)
				server.ResponseBody = []string{page}
				q := NewTableQuery("TEST/TABLE").Equal("ticker", "AA").Lt("c1", "11")
				q = q.Gt("c2", "12").Le("c3", "13").Ge("c4", "14").Columns("c1", "c3").PerPage(5)
				q = q.Cursor("abcd")
				it := q.Read(ctx)
				rows, err := rowsAll(it)
				So(err, ShouldBeNil)
				So(rows, ShouldResemble, expected)
				So(server.RequestPath, ShouldEqual, "/api/v3/datatables/TEST/TABLE.json")
				So(q.options.CursorID, ShouldEqual, "abcd")
				expectedQuery := q.Values()
				expectedQuery["api_key"] = []string{testKey}
				So(server.RequestQuery, ShouldResemble, expectedQuery)
				So(len(server.RequestQuery), ShouldEqual, 9)
			})

			Convey("fetches two pages", func() {
				expected := []*testRow{{42, "one"}, {84, "two"}, {96, "three"}, {101, "four"}}
				page1, err := TestTablePage(
					[][]Value{{42, "one"}, {84, "two"}}, testSchema, "nextpagecursor")
				So(err, ShouldBeNil)
				page2, err := TestTablePage(
					[][]Value{{96, "three"}, {101, "four"}}, testSchema, "")
				So(err, ShouldBeNil)
				server.ResponseBody = []string{page1, page2}
				it := NewTableQuery("TEST/TABLE").Read(ctx)
				rows, err := rowsAll(it)
				So(err, ShouldBeNil)
				So(rows, ShouldResemble, expected)
			})
		})

		Convey("Metadata", func() {
			metaJSON := `
				{"datatable":{
				 "vendor_code":"TEST",
				 "datatable_code":"TABLE",
				 "name":"A test table",
				 "description":null,
				 "columns":[
						{"name":"foo","type":"String"},
						{"name":"bar","type":"double"}],
				 "filters":["foo"],
				 "primary_key":["bar"],
				 "premium":true,
				 "status":{
						"refreshed_at":"2020-04-09T22:51:22.000Z",
						"status":"ON TIME",
						"expected_at":"*",
						"update_frequency":"CONTINUOUS"},
				 "data_version":{"code":"1","default":true,"description":null}
				}}`
			expected := TableMetadata{Datatable: DatatableMeta{
				VendorCode:  "TEST",
				TableCode:   "TABLE",
				Name:        "A test table",
				Description: "",
				Schema:      Schema{{Name: "foo", Type: "String"}, {Name: "bar", Type: "double"}},
				Filters:     []string{"foo"},
				PrimaryKey:  []string{"bar"},
				Premium:     true,
				Status: TableStatus{
					RefreshedAt:     *db.NewTime(2020, 4, 9, 22, 51, 22),
					Status:          "ON TIME",
					ExpectedAt:      "*",
					UpdateFrequency: "CONTINUOUS",
				},
				Version: TableVersion{
					Code:        "1",
					Default:     true,
					Description: "",
				},
			}}
			server.ResponseBody = []string{metaJSON}
			fetched, err := FetchTableMetadata(ctx, "TEST/TABLE")
			So(err, ShouldBeNil)
			So(fetched, ShouldResemble, &expected)
		})

		Convey("BulkDownload", func() {
			bulkJSON := `{
  "datatable_bulk_download": {
      "file": {
        "link": "https://test.url",
        "status": "regenerating",
        "data_snapshot_time": "2017-04-26 14:33:02 UTC"
      },
      "datatable": {
        "last_refreshed_time": "2017-10-12 09:03:36 UTC"
      }
    }
}`
			expected := TestBulkDownloadHandle("https://test.url", "regenerating",
				"2017-04-26 14:33:02 UTC", "2017-10-12 09:03:36 UTC")
			server.ResponseBody = []string{bulkJSON}
			h, err := BulkDownload(ctx, "TEST/TABLE")
			So(err, ShouldBeNil)
			So(h, ShouldResemble, expected)
		})
	})

	Convey("Schema methods work", t, func() {
		Convey("Equal", func() {
			orig := Schema{{"foo", "Integer"}, {"bar", "String"}}
			same := Schema{{"foo", "Integer"}, {"bar", "String"}}
			diffOrder := Schema{{"bar", "String"}, {"foo", "Integer"}}
			So(orig.Equal(same), ShouldBeTrue)
			So(orig.Equal(diffOrder), ShouldBeFalse)
		})

		Convey("SubsetOf", func() {
			orig := Schema{{"foo", "Integer"}, {"bar", "String"}}
			diffOrder := Schema{{"bar", "String"}, {"foo", "Integer"}}
			missingField := Schema{{"baz", "String"}, {"foo", "Integer"}}
			diff := Schema{{"bar", "Date"}, {"foo", "Integer"}}
			superset := Schema{{"bar", "String"}, {"baz", "Date"}, {"foo", "Integer"}}

			So(orig.SubsetOf(diffOrder), ShouldBeTrue)
			So(orig.SubsetOf(missingField), ShouldBeFalse)
			So(orig.SubsetOf(diff), ShouldBeFalse)
			So(orig.SubsetOf(superset), ShouldBeTrue)
		})

		Convey("MapFields", func() {
			s := Schema{
				{Name: "one", Type: "String"},
				{Name: "two", Type: "Integer"},
				{Name: "three", Type: "Date"},
			}
			So(s.MapFields(), ShouldResemble, map[string]int{"one": 0, "two": 1, "three": 2})
		})

		Convey("String", func() {
			s := Schema{{Name: "one", Type: "String"}, {Name: "two", Type: "Integer"}}
			So(s.String(), ShouldEqual, "{one: String, two: Integer}")
		})
	})
}
