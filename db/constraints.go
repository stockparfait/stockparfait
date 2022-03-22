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

// Constraints to filter the tickers and their time series.  Zero value means no
// constraints.
type Constraints struct {
	Tickers        map[string]struct{}
	ExcludeTickers map[string]struct{}
	Exchanges      map[string]struct{}
	Names          map[string]struct{}
	Categories     map[string]struct{}
	Sectors        map[string]struct{}
	Industries     map[string]struct{}
	Start          Date
	End            Date
}

// NewConstraints creates a new Constraints with no constraints.
func NewConstraints() *Constraints {
	return &Constraints{
		Tickers:        make(map[string]struct{}),
		ExcludeTickers: make(map[string]struct{}),
		Exchanges:      make(map[string]struct{}),
		Names:          make(map[string]struct{}),
		Categories:     make(map[string]struct{}),
		Sectors:        make(map[string]struct{}),
		Industries:     make(map[string]struct{}),
	}
}

// ExcludeTicker adds tickers to be ignored.
func (c *Constraints) ExcludeTicker(tickers ...string) *Constraints {
	for _, tk := range tickers {
		c.ExcludeTickers[tk] = struct{}{}
	}
	return c
}

// Ticker adds tickers to the constraints.
func (c *Constraints) Ticker(tickers ...string) *Constraints {
	for _, tk := range tickers {
		c.Tickers[tk] = struct{}{}
	}
	return c
}

// Exchange adds exchanges to the constraints.
func (c *Constraints) Exchange(ex ...string) *Constraints {
	for _, e := range ex {
		c.Exchanges[e] = struct{}{}
	}
	return c
}

// Name adds a company name to the constraints.
func (c *Constraints) Name(names ...string) *Constraints {
	for _, n := range names {
		c.Names[n] = struct{}{}
	}
	return c
}

// Category adds categories to the constraints.
func (c *Constraints) Category(cats ...string) *Constraints {
	for _, cat := range cats {
		c.Categories[cat] = struct{}{}
	}
	return c
}

// Sector adds sectors to the constraints.
func (c *Constraints) Sector(secs ...string) *Constraints {
	for _, s := range secs {
		c.Sectors[s] = struct{}{}
	}
	return c
}

// Industry adds industries to the constraints.
func (c *Constraints) Industry(inds ...string) *Constraints {
	for _, i := range inds {
		c.Industries[i] = struct{}{}
	}
	return c
}

// StartAt adds start date to the Constraints.
func (c *Constraints) StartAt(dt Date) *Constraints {
	c.Start = dt
	return c
}

// EndAt adds end date to the Constraints.
func (c *Constraints) EndAt(dt Date) *Constraints {
	c.End = dt
	return c
}

// CheckTicker whether it satisfies the constraints.
func (c *Constraints) CheckTicker(ticker string) bool {
	if len(c.ExcludeTickers) > 0 {
		if _, ok := c.ExcludeTickers[ticker]; ok {
			return false
		}
	}
	if len(c.Tickers) > 0 {
		if _, ok := c.Tickers[ticker]; !ok {
			return false
		}
	}
	return true
}

// CheckTickerRow whether it satisfies the constraints.
func (c *Constraints) CheckTickerRow(r TickerRow) bool {
	if len(c.Exchanges) > 0 {
		if _, ok := c.Exchanges[r.Exchange]; !ok {
			return false
		}
	}
	if len(c.Names) > 0 {
		if _, ok := c.Names[r.Name]; !ok {
			return false
		}
	}
	if len(c.Categories) > 0 {
		if _, ok := c.Categories[r.Category]; !ok {
			return false
		}
	}
	if len(c.Sectors) > 0 {
		if _, ok := c.Sectors[r.Sector]; !ok {
			return false
		}
	}
	if len(c.Industries) > 0 {
		if _, ok := c.Industries[r.Industry]; !ok {
			return false
		}
	}
	return true
}

// CheckDates checks that the date range is entirely within the constrained
// range. Both ends are inclusive.
func (c *Constraints) CheckDates(start, end Date) bool {
	if !c.Start.IsZero() && start.Before(c.Start) {
		return false
	}
	if !c.End.IsZero() && end.After(c.End) {
		return false
	}
	return true
}

// CheckAction whether it satisfies the constraints.
func (c *Constraints) CheckAction(r ActionRow) bool {
	return c.CheckDates(r.Date, r.Date)
}

// CheckPrice whether it satisfies the constraints.
func (c *Constraints) CheckPrice(r PriceRow) bool {
	return c.CheckDates(r.Date, r.Date)
}

// CheckResampled whether it satisfies the constraints.
func (c *Constraints) CheckResampled(r ResampledRow) bool {
	return c.CheckDates(r.DateOpen, r.DateClose)
}
