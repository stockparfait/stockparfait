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
	"fmt"

	"github.com/stockparfait/errors"
	"github.com/stockparfait/stockparfait/db"
	"github.com/stockparfait/stockparfait/message"
)

type Column struct {
	Kind string  `json:"kind" required:"true" choices:"ticker,name,exchange,category,sector,industry,price,volume"`
	Date db.Date `json:"date"` // required for "price" and "volume"
	Sort string  `json:"sort" choices:",ascending,descending"`
}

var _ message.Message = &Column{}

func (e *Column) InitMessage(js any) error {
	if err := message.Init(e, js); err != nil {
		return errors.Annotate(err, "failed to init Column")
	}
	switch e.Kind {
	case "price", "volume":
		if e.Date.IsZero() {
			return errors.Reason("date is required for kind=%s", e.Kind)
		}
	}
	return nil
}

func (e *Column) Header() string {
	switch e.Kind {
	case "ticker":
		return "Ticker"
	case "name":
		return "Name"
	case "exchange":
		return "Exchange"
	case "category":
		return "Category"
	case "sector":
		return "Sector"
	case "industry":
		return "Industry"
	case "price":
		return fmt.Sprintf("Split+Div Adjusted Close %s", e.Date)
	case "volume":
		return fmt.Sprintf("Cash Volume %s", e.Date)
	}
	return ""
}

type Config struct {
	Data    *db.Reader `json:"data" required:"true"`
	Columns []Column   `json:"columns"` // default: [{"kind": "ticker"}]
}

var _ message.Message = &Config{}

func (c *Config) InitMessage(js any) error {
	if err := message.Init(c, js); err != nil {
		return errors.Annotate(err, "failed to init Config")
	}
	if len(c.Columns) == 0 {
		c.Columns = []Column{{Kind: "ticker"}}
	}
	return nil
}
