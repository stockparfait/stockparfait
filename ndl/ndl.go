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
	"archive/zip"
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"strings"

	"github.com/stockparfait/errors"
	"github.com/stockparfait/fetch"
	"github.com/stockparfait/logging"
	"github.com/stockparfait/stockparfait/db"
)

type contextKey int

const (
	clientContextKey contextKey = iota
)

// URL is the default base URL of the server. It may be overwritten in tests
// before creating a new client.
var URL = "https://data.nasdaq.com/api/v3"

// Client for querying NDL tables and time-series.
type Client struct {
	baseURL string // the base URL of the server
	apiKey  string // your very own secret key
}

// newClient creates a new client.
func newClient(baseURL, apiKey string) *Client {
	return &Client{
		baseURL: baseURL,
		apiKey:  apiKey,
	}
}

// GetClient extracts the Client from the context, if any.
func GetClient(ctx context.Context) *Client {
	c, ok := ctx.Value(clientContextKey).(*Client)
	if !ok {
		return nil
	}
	return c
}

// UseClient creates a new client based on the API key and injects it into the
// context.
func UseClient(ctx context.Context, apiKey string) context.Context {
	return context.WithValue(ctx, clientContextKey, newClient(URL, apiKey))
}

// ValueLoader is the interface that a row type of a specific table must
// implement.
type ValueLoader interface {
	Load(v []Value, s Schema) error
}

// RowIterator iterates over query results row by row. Paging is handled
// transparently.
type RowIterator struct {
	context   context.Context
	query     *TableQuery
	page      tablePage
	index     int  // the data element for Next() to return
	pageCount int  // which page number we're on, for logging
	started   bool // if at least one Next call was ever made
}

// newRowIterator creates a new iterator.
func newRowIterator(ctx context.Context, query *TableQuery) *RowIterator {
	return &RowIterator{context: ctx, query: query}
}

// nextPage fetches and populates the iterator with the next page of data. When
// there are no more pages to load, or loading a page results in an error, the
// first return value becomes false.
func (it *RowIterator) nextPage() (bool, error) {
	if it.started && it.page.Meta.Cursor == "" {
		return false, nil
	}
	if it.started {
		it.query = it.query.Cursor(it.page.Meta.Cursor)
	}
	it.started = true
	// Clear the page, in case read doesn't overwrite some parts.
	it.page = tablePage{}
	if err := it.query.readPage(it.context, &it.page); err != nil {
		return false, errors.Annotate(err, "failed to query page %d", it.pageCount+1)
	}
	it.index = 0
	it.pageCount++
	logging.Infof(it.context,
		"Nasdaq Data Link: fetched page %d with %d rows; cursor: %s",
		it.pageCount, len(it.page.Datatable.Data), it.page.Meta.Cursor)
	return true, nil
}

// Next loads the next row. If there are no more rows, the second value is
// false. Note, that error may be non-nil regardless of the end of iterator.
func (it *RowIterator) Next(row ValueLoader) (bool, error) {
	if it.query == nil {
		return false, nil
	}
	if !it.started {
		if ok, err := it.nextPage(); !ok {
			return false, err
		}
	}
	if it.index >= len(it.page.Datatable.Data) {
		if ok, err := it.nextPage(); !ok {
			return false, err
		}
	}
	if it.index >= len(it.page.Datatable.Data) {
		return false, nil
	}
	err := row.Load(it.page.Datatable.Data[it.index], it.page.Datatable.Schema)
	it.index++
	if err != nil {
		return true, errors.Annotate(err, "failed to parse row %d in page %d",
			it.index, it.pageCount)
	}
	return true, nil
}

// TableQuery is a builder for a table query.
type TableQuery struct {
	table   string // a fully qualified table name, e.g. SHARADAR/SEP
	filters []queryFilter
	options queryOptions
}

// Copy creates a deep copy of the query. It is primarily used in its builder
// methods.
func (q *TableQuery) Copy() *TableQuery {
	q2 := TableQuery{table: q.table, options: q.options}
	q2.filters = make([]queryFilter, len(q.filters))
	for i, f := range q.filters {
		q2.filters[i] = f
	}
	return &q2
}

// queryFilterKind is the enum for different filters.
type queryFilterKind string

// Values for the queryFilterKind.
const (
	queryFilterEq = queryFilterKind("")
	queryFilterLt = queryFilterKind(".lt")
	queryFilterGt = queryFilterKind(".gt")
	queryFilterLe = queryFilterKind(".lte")
	queryFilterGe = queryFilterKind(".gte")
)

// queryFilter is a single filter used in a query.
type queryFilter struct {
	Kind   queryFilterKind
	Column string
	Values []string // only QueryFilterEqual can have multiple values
}

// queryOptions are options common to all the tables.
type queryOptions struct {
	Columns  []string // if non-nil, return only these columns
	PerPage  int      // number of results per page, up to 10000 max (0 = default size)
	CursorID string   // next page cursor
}

// NewTableQuery creates a new query.
func NewTableQuery(table string) *TableQuery {
	q := TableQuery{table: table}
	return &q
}

// Value is an arbitrary value of a table cell.
type Value interface{}

// SchemaField is the schema definition for a single table column.
type SchemaField struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// Schema definition for a table.
type Schema []SchemaField

// Equal tests two schemas for exact equality, including the field ordering.
func (s Schema) Equal(s2 Schema) bool {
	for i, f := range s {
		if f != s2[i] {
			return false
		}
	}
	return true
}

// SubsetOf tests if self is a subset of the other schema. This is useful for
// robust ValueLoader's that can continue to work when the schema adds new
// fields.
func (s Schema) SubsetOf(s2 Schema) bool {
	m := make(map[string]string)
	for _, f := range s2 {
		m[f.Name] = f.Type
	}
	for _, f := range s {
		if tp2, ok := m[f.Name]; !ok || f.Type != tp2 {
			return false
		}
	}
	return true
}

// MapFields creates a map of {field name -> field index} in the schema.
func (s Schema) MapFields() map[string]int {
	res := make(map[string]int)
	for i, f := range s {
		res[f.Name] = i
	}
	return res
}

// String prints a string representation of the schema.
func (s Schema) String() string {
	fields := []string{}
	for _, f := range s {
		fields = append(fields, fmt.Sprintf("%s: %s", f.Name, f.Type))
	}
	return "{" + strings.Join(fields, ", ") + "}"
}

// datatable holds the data and the schema of a table page.
type datatable struct {
	Data   [][]Value `json:"data"`
	Schema Schema    `json:"columns"`
}

// metadata for a table page.
type metadata struct {
	Cursor string `json:"next_cursor_id,omitempty"`
}

// tablePage is the format of a single page of table data.
type tablePage struct {
	Datatable datatable `json:"datatable"`
	Meta      metadata  `json:"meta,omitempty"`
}

// TestTablePage generates the JSON string in a format as returned by the NDL
// Table API. For use in tests.
func TestTablePage(data [][]Value, schema Schema, cursor string) (string, error) {
	bytes, err := json.Marshal(&tablePage{
		Datatable: datatable{Data: data, Schema: schema},
		Meta:      metadata{Cursor: cursor},
	})
	return string(bytes), err
}

// readPage executes the query using the Client from the context and downloads
// one page of data.
func (q *TableQuery) readPage(ctx context.Context, page *tablePage) error {
	client := GetClient(ctx)
	if client == nil {
		return errors.Reason("TableQuery.Read: no client in context")
	}
	uri := client.baseURL + "/datatables/" + q.Path() + ".json"
	query := q.Values()
	query["api_key"] = []string{client.apiKey}

	if err := fetch.FetchJSON(ctx, uri, page, query, nil); err != nil {
		return errors.Annotate(err, "TableQuery.Read: failed to fetch URL")
	}
	return nil
}

// Read sets up the iterator over the result rows, which will execute the query
// as needed and handle paging transparently.
func (q *TableQuery) Read(ctx context.Context) *RowIterator {
	return newRowIterator(ctx, q)
}

// Equal adds an equality filter: the value of the column must equal one of the
// given values. This and other builder methods always create a deep copy of the
// query, leaving the original intact.
func (q *TableQuery) Equal(column string, values ...string) *TableQuery {
	q2 := q.Copy()
	q2.filters = append(q2.filters, queryFilter{queryFilterEq, column, values})
	return q2
}

// compare adds a comparison filter.
func compare(q *TableQuery, column string, kind queryFilterKind, value string) *TableQuery {
	q2 := q.Copy()
	q2.filters = append(q2.filters, queryFilter{
		kind, column, []string{value}})
	return q2
}

// Lt adds a strict inequality filter: a numerical column's value must be < value.
func (q *TableQuery) Lt(column string, value string) *TableQuery {
	return compare(q, column, queryFilterLt, value)
}

// Gt adds a strict inequality filter: a numerical column's value must be > value.
func (q *TableQuery) Gt(column string, value string) *TableQuery {
	return compare(q, column, queryFilterGt, value)
}

// Le adds a strict inequality filter: a numerical column's value must be <= value.
func (q *TableQuery) Le(column string, value string) *TableQuery {
	return compare(q, column, queryFilterLe, value)
}

// Ge adds a strict inequality filter: a numerical column's value must be >= value.
func (q *TableQuery) Ge(column string, value string) *TableQuery {
	return compare(q, column, queryFilterGe, value)
}

// Columns constraints the query result to only these columns.
func (q *TableQuery) Columns(columns ...string) *TableQuery {
	q2 := q.Copy()
	q2.options.Columns = columns
	return q2
}

// PerPage sets the maximum number of results in a single response, [0..10000].
func (q *TableQuery) PerPage(size int) *TableQuery {
	if size < 0 {
		size = 0
	}
	if size > 10000 {
		size = 10000
	}
	q2 := q.Copy()
	q2.options.PerPage = size
	return q2
}

// Cursor sets the cursor ID for a paging query.
func (q *TableQuery) Cursor(cursor string) *TableQuery {
	q2 := q.Copy()
	q2.options.CursorID = cursor
	return q2
}

// Path returns the URL path to add to the base URL.
func (q *TableQuery) Path() string {
	return q.table
}

// Values returns the query values for the query. Each call creates a new
// object, so the caller is free to modify it without affecting the query.
func (q *TableQuery) Values() url.Values {
	v := make(url.Values)
	for _, f := range q.filters {
		v[f.Column+string(f.Kind)] = []string{strings.Join(f.Values, ",")}
	}
	if q.options.Columns != nil {
		v["qopts.columns"] = []string{strings.Join(q.options.Columns, ",")}
	}
	if q.options.PerPage != 0 {
		v["qopts.per_page"] = []string{fmt.Sprintf("%d", q.options.PerPage)}
	}
	if q.options.CursorID != "" {
		v["qopts.cursor_id"] = []string{q.options.CursorID}
	}
	return v
}

// TableStatus is a part of DatatableMeta.
type TableStatus struct {
	RefreshedAt     db.Time `json:"refreshed_at"`
	Status          string  `json:"status"`
	ExpectedAt      string  `json:"expected_at"`
	UpdateFrequency string  `json:"update_frequency"`
}

// TableVersion is a part of DatatableMeta.
type TableVersion struct {
	Code        string `json:"code"`
	Default     bool   `json:"default"`
	Description string `json:"description"`
}

// DatatableMeta is the JSON struct for the table metadata.
type DatatableMeta struct {
	VendorCode  string       `json:"vendor_code"`
	TableCode   string       `json:"datatable_code"`
	Name        string       `json:"name"`
	Description string       `json:"description"`
	Schema      Schema       `json:"columns"`
	Filters     []string     `json:"filters"`
	PrimaryKey  []string     `json:"primary_key"`
	Premium     bool         `json:"premium"`
	Status      TableStatus  `json:"status"`
	Version     TableVersion `json:"data_version"`
}

// TableMetadata is the format returned by the metadata API.
type TableMetadata struct {
	Datatable DatatableMeta `json:"datatable"`
}

// FetchTableMetadata obtains metadata about the requested table specified as
// PUBLISHER/TABLE.
func FetchTableMetadata(ctx context.Context, table string) (*TableMetadata, error) {
	var tm TableMetadata
	client := GetClient(ctx)
	if client == nil {
		return nil, errors.Reason("no client in context")
	}
	uri := client.baseURL + "/datatables/" + table + "/metadata.json"
	query := make(url.Values)
	query["api_key"] = []string{client.apiKey}
	if err := fetch.FetchJSON(ctx, uri, &tm, query, nil); err != nil {
		return nil, errors.Annotate(err, "failed to fetch URL")
	}
	return &tm, nil
}

// bulkDownloadHandle is the JSON schema received by the first asynchronous bulk
// download call.
type bulkDownloadHandle struct {
	Data struct {
		File struct {
			Link         string `json:"link"`
			Status       string `json:"status"`
			SnapshotTime string `json:"data_snapshot_time"`
		} `json:"file"`
		Datatable struct {
			LastRefreshedTime string `json:"last_refreshed_time"`
		} `json:"datatable"`
	} `json:"datatable_bulk_download"`
}

// Values of the Status field of BulkDownloadHandle.
const (
	StatusFresh        = "fresh"
	StatusRegenerating = "regenerating"
	StatusCreating     = "creating"
)

// BulkDownloadHandle is a simplified result of the first asynchronous bulk
// download call.
type BulkDownloadHandle struct {
	Link              string
	Status            string
	SnapshotTime      string
	LastRefreshedTime string
	testCloser        io.Closer // used in tests
}

// BulkDownload receives the bulk download metadata with the data link.
func BulkDownload(ctx context.Context, table string) (*BulkDownloadHandle, error) {
	var h bulkDownloadHandle
	client := GetClient(ctx)
	if client == nil {
		return nil, errors.Reason("no client in context")
	}
	uri := client.baseURL + "/datatables/" + table + ".json"
	query := make(url.Values)
	query["api_key"] = []string{client.apiKey}
	query["qopts.export"] = []string{"true"}
	if err := fetch.FetchJSON(ctx, uri, &h, query, nil); err != nil {
		return nil, errors.Annotate(err, "failed to fetch URL")
	}
	b := BulkDownloadHandle{
		Link:              h.Data.File.Link,
		Status:            h.Data.File.Status,
		SnapshotTime:      h.Data.File.SnapshotTime,
		LastRefreshedTime: h.Data.Datatable.LastRefreshedTime,
	}
	return &b, nil
}

// CSVReader implements a streaming CSV reader, one row at a time, with a
// Close() method to release its resources.
type CSVReader struct {
	reader              *csv.Reader
	closers             []io.Closer
	ignoreDeferredClose bool // see deferredClose method
}

// Read the next CSV row as a slice of strings. It returns the same errors as
// encoding/csv.Reader.Read() method. In particular, it returns nil, io.EOF when
// there are no more rows.
func (r *CSVReader) Read() ([]string, error) {
	return r.reader.Read()
}

// Close CSVReader and release all the resources.
func (r *CSVReader) Close() {
	// Must invoke closers in reverse order. Ignore their errors.
	for i := len(r.closers) - 1; i >= 0; i-- {
		r.closers[i].Close()
		r.closers = r.closers[0:i]
	}
}

// deferredClose is to be used in defer in BulkDownloadCSV. When an intermediate
// error occurs, it is important to release all of the already registered
// closers before returning an error, but not if the method terminates normally.
func (r *CSVReader) deferredClose() {
	if r.ignoreDeferredClose {
		return
	}
	r.Close()
}

// AddCloser to the list of closers. Method Close() will call each registered
// closer in LIFO order.
func (r *CSVReader) AddCloser(c io.Closer) {
	r.closers = append(r.closers, c)
}

// BulkDownloadCSV starts downloading the actual data pointed to by
// BulkDownloadHandle. It downloads the zip archive with a single CSV file into
// memory, and returns a CSVReader which streams the contents of that file.
// Make sure to call CSVReader.Close() when done with the CSV stream.
func BulkDownloadCSV(ctx context.Context, h *BulkDownloadHandle) (*CSVReader, error) {
	var csvReader CSVReader
	defer csvReader.deferredClose()

	resp, err := fetch.GetRetry(ctx, h.Link, nil, nil)
	if err != nil {
		return nil, errors.Annotate(err, "failed to initiate download")
	}
	csvReader.AddCloser(resp.Body)
	if h.testCloser != nil { // used in tests to verify that CSVReader was closed
		csvReader.AddCloser(h.testCloser)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Annotate(err, "failed to read response body")
	}
	r := bytes.NewReader(data)
	z, err := zip.NewReader(r, r.Size())
	if err != nil {
		return nil, errors.Annotate(err, "failed to read zip archive")
	}
	if len(z.File) != 1 {
		names := make([]string, len(z.File))
		for i := 0; i < len(z.File); i++ {
			names[i] = z.File[i].Name
		}
		return nil, errors.Reason("archive contains %d files (expected 1):\n  %s",
			len(z.File), strings.Join(names, "\n  "))
	}
	rc, err := z.File[0].Open()
	if err != nil {
		return nil, errors.Annotate(err,
			"failed to open file in archive '%s'", z.File[0].Name)
	}
	csvReader.AddCloser(rc)
	csvReader.reader = csv.NewReader(rc)
	csvReader.ignoreDeferredClose = true
	return &csvReader, nil
}
