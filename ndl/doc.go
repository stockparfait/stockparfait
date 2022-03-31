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

// Package ndl implements generic table API of Nasdaq Data Link (NDL).
//
// Official documentation is at https://docs.data.nasdaq.com/docs/tables-1 .
//
// Each NDL table has a schema, which is the list of column names and their
// types, in the order they appear in the table. This schema can be obtained for
// the originial table using FetchTableMetadata(). The relevant schema is also
// included in each downloaded table page, which may be a subset of the full
// schema if only a subset of columns was requested.
//
// The raw NDL API can return only up to 10K rows in a single page. However, the
// JSON format used in this package includes a cursor for the next page, thus
// allowing paging when downloading more than 10K rows. This package implements
// transparent paging in RowIterator.
//
// APIs for specific providers and products, such as Sharadar Equities and ETFs,
// are implemented in the subpackages.
package ndl
