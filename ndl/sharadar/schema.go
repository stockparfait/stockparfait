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

package sharadar

import (
	"strconv"
	"strings"

	"github.com/stockparfait/errors"
	"github.com/stockparfait/stockparfait/db"
	"github.com/stockparfait/stockparfait/ndl"
)

// Scale categorises dollar value scale as follows:
//
// 1 - Nano <$50m;
// 2 - Micro < $300m;
// 3 - Small < $2bn;
// 4 - Mid <$10bn;
// 5 - Large < $200bn;
// 6 - Mega >= $200bn.
//
// This categorization is experimental and is subject to change.
type Scale uint8

const (
	ScaleUnset Scale = iota
	ScaleNano
	ScaleMicro
	ScaleSmall
	ScaleMid
	ScaleLarge
	ScaleMega
)

var str2scale = map[string]Scale{
	"1 - Nano":  ScaleNano,
	"2 - Micro": ScaleMicro,
	"3 - Small": ScaleSmall,
	"4 - Mid":   ScaleMid,
	"5 - Large": ScaleLarge,
	"6 - Mega":  ScaleMega,
}

// Ticker is a row in the TICKERS table.
type Ticker struct {
	TableName      string   //
	Permaticker    int      // Sharadar's unique ID
	Ticker         string   // uniquified by Sharadar if reused
	Name           string   // the company's name
	Exchange       string   // e.g. NYSE
	IsDelisted     bool     //
	Category       string   // e.g. "Domestic", "Canadian", "ADR"
	CUSIPs         []string // security identifier(s)
	SICCode        uint16   // Standard Industrial Classification code
	SICSector      string   //
	SICIndustry    string   //
	FAMASector     string   // currently always empty
	FAMAIndustry   string   //
	Sector         string   //
	Industry       string   //
	ScaleMarketCap Scale    //
	ScaleRevenue   Scale    //
	RelatedTickers []string // e.g. previously used tickers by this company
	Currency       string   // e.g. "USD"
	Location       string   //
	LastUpdated    db.Date  //
	FirstAdded     db.Date  //
	FirstPriceDate db.Date  // min 1986-01-01; approx. IPO
	LastPriceDate  db.Date  // latest price available
	FirstQuarter   db.Date  //
	LastQuarter    db.Date  //
	SECFilings     string   // URL
	CompanySite    string   // URL
}

var _ ndl.ValueLoader = &Ticker{}

// TickerSchema is the expected schema for the TICKERS table.
var TickerSchema = ndl.Schema{
	{Name: "table", Type: "String"},
	{Name: "permaticker", Type: "Integer"},
	{Name: "ticker", Type: "String"},
	{Name: "name", Type: "String"},
	{Name: "exchange", Type: "String"},
	{Name: "isdelisted", Type: "String"},
	{Name: "category", Type: "String"},
	{Name: "cusips", Type: "String"},
	{Name: "siccode", Type: "Integer"},
	{Name: "sicsector", Type: "String"},
	{Name: "sicindustry", Type: "String"},
	{Name: "famasector", Type: "String"},
	{Name: "famaindustry", Type: "String"},
	{Name: "sector", Type: "String"},
	{Name: "industry", Type: "String"},
	{Name: "scalemarketcap", Type: "String"},
	{Name: "scalerevenue", Type: "String"},
	{Name: "relatedtickers", Type: "String"},
	{Name: "currency", Type: "String"},
	{Name: "location", Type: "String"},
	{Name: "lastupdated", Type: "Date"},
	{Name: "firstadded", Type: "Date"},
	{Name: "firstpricedate", Type: "Date"},
	{Name: "lastpricedate", Type: "Date"},
	{Name: "firstquarter", Type: "String"},
	{Name: "lastquarter", Type: "String"},
	{Name: "secfilings", Type: "String"},
	{Name: "companysite", Type: "String"},
}

// ActionType is the enum for the actions.
type ActionType uint8

// Enum values for ActionType.
const (
	UnknownAction ActionType = iota
	AcquisitionByAction
	AcquisitionOfAction
	BankruptcyLiquidationAction
	DelistedAction
	DividendAction // cash dividends adjusted for splits and dividends
	InitiatedAction
	ListedAction
	MergerFromAction
	MergerToAction
	RegulatoryDelistingAction
	RelationAction
	SpinoffAction
	SpinoffDividendAction
	SplitAction // stock split or stock dividend
	SpunoffFromAction
	TickerChangeFromAction
	TickerChangeToAction
	VoluntaryDelistingAction
)

var AdjustmentActions = []ActionType{
	DividendAction,
	SpinoffDividendAction,
	SplitAction,
}

var string2action = map[string]ActionType{
	"acquisitionby":         AcquisitionByAction,
	"acquisitionof":         AcquisitionOfAction,
	"bankruptcyliquidation": BankruptcyLiquidationAction,
	"delisted":              DelistedAction,
	"dividend":              DividendAction,
	"initiated":             InitiatedAction,
	"listed":                ListedAction,
	"mergerfrom":            MergerFromAction,
	"mergerto":              MergerToAction,
	"regulatorydelisting":   RegulatoryDelistingAction,
	"relation":              RelationAction,
	"spinoff":               SpinoffAction,
	"spinoffdividend":       SpinoffDividendAction,
	"split":                 SplitAction,
	"spunofffrom":           SpunoffFromAction,
	"tickerchangefrom":      TickerChangeFromAction,
	"tickerchangeto":        TickerChangeToAction,
	"voluntarydelisting":    VoluntaryDelistingAction,
}

// Set assigns the action a value based on the input string.
func (at *ActionType) Set(s string) {
	a, ok := string2action[s]
	if !ok {
		*at = UnknownAction
		return
	}
	*at = a
}

// String converts the enum value to a string.
func (at *ActionType) String() string {
	for s, a := range string2action {
		if *at == a {
			return s
		}
	}
	return "unknown"
}

// Action is a row in the ACTIONS table. Value field depends on the action:
// - DividendAction: split and split-dividend adjusted cash dividend amount.
// - SpinoffDividendAction: dollar value of the shares of the spunoff company
//   issued for each share of the parent company.
// - SplitAction: the number of resulting shares per each original share.
type Action struct {
	Date         db.Date
	Action       ActionType
	Ticker       string  // the uniquified ticker in the DB
	Name         string  // the Ticker's current company name
	Value        float32 // depends of the Action
	ContraTicker string  // depends on the Action
	ContraName   string  // the ContraTicker's current company name
}

var _ ndl.ValueLoader = &Action{}

// TestAction creates an Action for use in tests.
func TestAction(date db.Date, action ActionType, ticker string, value float32) Action {
	return Action{
		Date:   date,
		Action: action,
		Ticker: ticker,
		Value:  value,
	}
}

// ActionSchema is the expected schema for the ACTIONS table.
var ActionSchema = ndl.Schema{
	{Name: "date", Type: "Date"},
	{Name: "action", Type: "String"},
	{Name: "ticker", Type: "String"},
	{Name: "name", Type: "String"}, // same as in Ticker
	{Name: "value", Type: "BigDecimal(20,5)"},
	{Name: "contraticker", Type: "String"}, // old/new ticker name
	{Name: "contraname", Type: "String"},   // old/new company name
}

// Price is a row in the SEP/SFP table.
type Price struct {
	Ticker string
	Date   db.Date
	// All OHLCV values are adjusted for stock splits and stock dividends, but not
	// for cash dividends or spinoffs.
	Open            float32
	High            float32
	Low             float32
	Close           float32
	Volume          float32
	CloseUnadjusted float32
	// Adjusted for stock splits, cash dividends and spinoffs.
	CloseAdjusted float32
	LastUpdated   db.Date
}

var _ ndl.ValueLoader = &Price{}

// PriceSchema is the expected schema for the SEP table.
var PriceSchema = ndl.Schema{
	{Name: "ticker", Type: "text"},
	{Name: "date", Type: "Date"},
	{Name: "open", Type: "double"},
	{Name: "high", Type: "double"},
	{Name: "low", Type: "double"},
	{Name: "close", Type: "double"},
	{Name: "volume", Type: "double"},
	{Name: "closeadj", Type: "double"}, // added on Mar 29, 2021
	{Name: "closeunadj", Type: "double"},
	{Name: "lastupdated", Type: "Date"},
}

func typeErr(v ndl.Value, tp string) error {
	return errors.Reason("expected %s but found %T: %v", tp, v, v)
}

func value2str(v ndl.Value) (string, error) {
	if v == nil {
		return "", nil
	}
	if str, ok := v.(string); ok {
		return str, nil
	}
	return "", typeErr(v, "a string")
}

func value2num(v ndl.Value) (float32, error) {
	if v == nil {
		return 0.0, nil
	}
	if num, ok := v.(float64); ok { // JSON numbers always unmarshal to float64
		return float32(num), nil
	}
	return 0.0, typeErr(v, "a number")
}

func value2date(v ndl.Value) (db.Date, error) {
	if v == nil {
		return db.Date{}, nil
	}
	str, ok := v.(string)
	if !ok {
		return db.Date{}, typeErr(v, "a date string")
	}
	return db.NewDateFromString(str)
}

func value2bool(v ndl.Value) (bool, error) {
	if v == nil {
		return false, nil
	}
	str, ok := v.(string)
	if !ok {
		return false, typeErr(v, "a Y/N string")
	}
	switch str {
	case "N":
		return false, nil
	case "Y":
		return true, nil
	}
	return false, typeErr(v, "a Y/N string")
}

func value2scale(v ndl.Value) (Scale, error) {
	if v == nil {
		return ScaleUnset, nil
	}
	str, ok := v.(string)
	if !ok {
		return ScaleUnset, typeErr(v, "a scale string")
	}
	if str == "" {
		return ScaleUnset, nil
	}
	scale, ok := str2scale[str]
	if !ok {
		return ScaleUnset, typeErr(v, "a scale string")
	}
	return scale, nil
}

// Load implements ndl.ValueLoader.
func (r *Ticker) Load(v []ndl.Value, s ndl.Schema) error {
	if !TickerSchema.SubsetOf(s) {
		return errors.Reason("unexpected schema: %s", s.String())
	}
	if len(v) != len(s) {
		return errors.Reason("expected %d values, received %d: %v", len(s), len(v), v)
	}
	m := s.MapFields()
	var num float32
	var err error

	v2str := func(field string) (string, error) {
		return value2str(v[m[field]])
	}
	v2strs := func(field string) ([]string, error) {
		str, err := value2str(v[m[field]])
		if err != nil {
			return nil, err
		}
		return strings.Split(str, " "), nil
	}
	v2num := func(field string) (float32, error) {
		return value2num(v[m[field]])
	}
	v2date := func(field string) (db.Date, error) {
		return value2date(v[m[field]])
	}
	v2bool := func(field string) (bool, error) {
		return value2bool(v[m[field]])
	}
	v2scale := func(field string) (Scale, error) {
		return value2scale(v[m[field]])
	}

	if r.TableName, err = v2str("table"); err != nil {
		return errors.Annotate(err, "table should be a string")
	}
	if num, err = v2num("permaticker"); err != nil {
		return errors.Annotate(err, "permaticker should be a number")
	}
	if num == 0.0 {
		return errors.Reason("permaticker is invalid: %v", v[m["permaticker"]])
	}
	r.Permaticker = int(num)
	if r.Ticker, err = v2str("ticker"); err != nil {
		return errors.Annotate(err, "ticker should be a string")
	}
	if r.Name, err = v2str("name"); err != nil {
		return errors.Annotate(err, "name should be a string")
	}
	if r.Exchange, err = v2str("exchange"); err != nil {
		return errors.Annotate(err, "exchange should be a string")
	}
	if r.IsDelisted, err = v2bool("isdelisted"); err != nil {
		return errors.Annotate(err, "isdelisted should be a Y/N string")
	}
	if r.Category, err = v2str("category"); err != nil {
		return errors.Annotate(err, "category should be a string")
	}
	if r.CUSIPs, err = v2strs("cusips"); err != nil {
		return errors.Annotate(err, "cusips should be a space-separated string list")
	}
	if num, err = v2num("siccode"); err != nil {
		return errors.Annotate(err, "siccode should be a number")
	}
	r.SICCode = uint16(num)
	if r.SICSector, err = v2str("sicsector"); err != nil {
		return errors.Annotate(err, "sicsector should be a string")
	}
	if r.SICIndustry, err = v2str("sicindustry"); err != nil {
		return errors.Annotate(err, "sicindustry should be a string")
	}
	if r.FAMASector, err = v2str("famasector"); err != nil {
		return errors.Annotate(err, "famasector should be a string")
	}
	if r.FAMAIndustry, err = v2str("famaindustry"); err != nil {
		return errors.Annotate(err, "famaindustry should be a string")
	}
	if r.Sector, err = v2str("sector"); err != nil {
		return errors.Annotate(err, "sector should be a string")
	}
	if r.Industry, err = v2str("industry"); err != nil {
		return errors.Annotate(err, "industry should be a string")
	}
	if r.ScaleMarketCap, err = v2scale("scalemarketcap"); err != nil {
		return errors.Annotate(err, "scalemarketcap should be a scale string")
	}
	if r.ScaleRevenue, err = v2scale("scalerevenue"); err != nil {
		return errors.Annotate(err, "scalerevenue should be a scale string")
	}
	if r.RelatedTickers, err = v2strs("relatedtickers"); err != nil {
		return errors.Annotate(err,
			"relatedtickers should be a space-separated string list")
	}
	if r.Currency, err = v2str("currency"); err != nil {
		return errors.Annotate(err, "currency should be a string")
	}
	if r.Location, err = v2str("location"); err != nil {
		return errors.Annotate(err, "location should be a string")
	}
	if r.LastUpdated, err = v2date("lastupdated"); err != nil {
		return errors.Annotate(err, "lastupdated should be a date string")
	}
	if r.FirstAdded, err = v2date("firstadded"); err != nil {
		return errors.Annotate(err, "firstadded should be a date string")
	}
	if r.FirstPriceDate, err = v2date("firstpricedate"); err != nil {
		return errors.Annotate(err, "firstpricedate should be a date string")
	}
	if r.LastPriceDate, err = v2date("lastpricedate"); err != nil {
		return errors.Annotate(err, "lastpricedate should be a date string")
	}
	if r.FirstQuarter, err = v2date("firstquarter"); err != nil {
		return errors.Annotate(err, "firstquarter should be a date string")
	}
	if r.LastQuarter, err = v2date("lastquarter"); err != nil {
		return errors.Annotate(err, "lastquarter should be a date string")
	}
	if r.SECFilings, err = v2str("secfilings"); err != nil {
		return errors.Annotate(err, "secfilings should be a URL string")
	}
	if r.CompanySite, err = v2str("companysite"); err != nil {
		return errors.Annotate(err, "companysite should be a URL string")
	}
	return nil
}

// Is checks if the action is of one of the given types.
func (r *Action) Is(types ...ActionType) bool {
	for _, t := range types {
		if r.Action == t {
			return true
		}
	}
	return false
}

// Load implements ndl.ValueLoader.
func (r *Action) Load(v []ndl.Value, s ndl.Schema) error {
	if !ActionSchema.SubsetOf(s) {
		return errors.Reason("unexpected schema: %s", s.String())
	}
	if len(v) != len(s) {
		return errors.Reason("expected %d values, received %d: %v", len(s), len(v), v)
	}
	m := s.MapFields()
	var str string
	var err error

	v2str := func(field string) (string, error) {
		return value2str(v[m[field]])
	}
	v2num := func(field string) (float32, error) {
		return value2num(v[m[field]])
	}
	v2date := func(field string) (db.Date, error) {
		return value2date(v[m[field]])
	}

	if r.Date, err = v2date("date"); err != nil {
		return errors.Annotate(err, "date should be a date string")
	}
	if str, err = v2str("action"); err != nil {
		return errors.Annotate(err, "action should be a string")
	}
	r.Action.Set(str)
	if r.Ticker, err = v2str("ticker"); err != nil {
		return errors.Annotate(err, "ticker should be a string")
	}
	if r.Name, err = v2str("name"); err != nil {
		return errors.Annotate(err, "name should be a string")
	}
	if r.Value, err = v2num("value"); err != nil {
		return errors.Annotate(err, "value should be a number")
	}
	if r.ContraTicker, err = v2str("contraticker"); err != nil {
		return errors.Annotate(err, "contraticker should be a string")
	}
	if r.ContraName, err = v2str("contraname"); err != nil {
		return errors.Annotate(err, "contraname should be a string")
	}
	return nil
}

// Load implements ndl.ValueLoader.
func (r *Price) Load(v []ndl.Value, s ndl.Schema) error {
	if !PriceSchema.SubsetOf(s) {
		return errors.Reason("unexpected schema: %s", s.String())
	}
	if len(v) != len(s) {
		return errors.Reason("expected %d values, received %d: %v", len(s), len(v), v)
	}
	m := s.MapFields()
	var err error

	v2str := func(field string) (string, error) {
		return value2str(v[m[field]])
	}
	v2num := func(field string) (float32, error) {
		return value2num(v[m[field]])
	}
	v2date := func(field string) (db.Date, error) {
		return value2date(v[m[field]])
	}

	if r.Ticker, err = v2str("ticker"); err != nil {
		return errors.Annotate(err, "ticker should be a string")
	}
	if r.Date, err = v2date("date"); err != nil {
		return errors.Annotate(err, "date should be a date string")
	}
	if r.Open, err = v2num("open"); err != nil {
		return errors.Annotate(err, "open should be a number")
	}
	if r.High, err = v2num("high"); err != nil {
		return errors.Annotate(err, "high should be a number")
	}
	if r.Low, err = v2num("low"); err != nil {
		return errors.Annotate(err, "low should be a number")
	}
	if r.Close, err = v2num("close"); err != nil {
		return errors.Annotate(err, "close should be a number")
	}
	if r.Volume, err = v2num("volume"); err != nil {
		return errors.Annotate(err, "volume should be a number")
	}
	if r.CloseUnadjusted, err = v2num("closeunadj"); err != nil {
		return errors.Annotate(err, "closeunadj should be a number")
	}
	if r.CloseAdjusted, err = v2num("closeadj"); err != nil {
		return errors.Annotate(err, "closeadj should be a number")
	}
	if r.LastUpdated, err = v2date("lastupdated"); err != nil {
		return errors.Annotate(err, "lastupdated should be a date")
	}
	return nil
}

// FromCSV sets the value of Price from a CSV row based on a column map {field
// name -> column number}, where the field names are as in the PriceSchema.
func (r *Price) FromCSV(row []string, columnMap map[string]int) error {
	if len(row) != len(columnMap) {
		return errors.Reason("expected %d columns, received %d: %v",
			len(columnMap), len(row), row)
	}

	r.Ticker = row[columnMap["ticker"]]

	var err error
	r.Date, err = db.NewDateFromString(row[columnMap["date"]])
	if err != nil {
		return errors.Annotate(err, "date must be a date string: '%s'",
			row[columnMap["date"]])
	}

	v, err := strconv.ParseFloat(row[columnMap["open"]], 32)
	if err != nil {
		return errors.Annotate(err, "open should be a number: %v",
			row[columnMap["open"]])
	}
	r.Open = float32(v)

	v, err = strconv.ParseFloat(row[columnMap["high"]], 32)
	if err != nil {
		return errors.Annotate(err, "high should be a number: %v",
			row[columnMap["high"]])
	}
	r.High = float32(v)

	v, err = strconv.ParseFloat(row[columnMap["low"]], 32)
	if err != nil {
		return errors.Annotate(err, "low should be a number: %v",
			row[columnMap["low"]])
	}
	r.Low = float32(v)

	v, err = strconv.ParseFloat(row[columnMap["close"]], 32)
	if err != nil {
		return errors.Annotate(err, "close should be a number: %v",
			row[columnMap["close"]])
	}
	r.Close = float32(v)

	v, err = strconv.ParseFloat(row[columnMap["volume"]], 32)
	if err != nil {
		return errors.Annotate(err, "volume should be a number: %v",
			row[columnMap["volume"]])
	}
	r.Volume = float32(v)

	v, err = strconv.ParseFloat(row[columnMap["closeunadj"]], 32)
	if err != nil {
		return errors.Annotate(err, "closeunadj should be a number: %v",
			row[columnMap["closeunadj"]])
	}
	r.CloseUnadjusted = float32(v)

	v, err = strconv.ParseFloat(row[columnMap["closeadj"]], 32)
	if err != nil {
		return errors.Annotate(err, "closeadj should be a number: %v",
			row[columnMap["closeadj"]])
	}
	r.CloseAdjusted = float32(v)

	r.LastUpdated, err = db.NewDateFromString(row[columnMap["lastupdated"]])
	if err != nil {
		return errors.Annotate(err, "lastupdated must be a date string: '%s'",
			row[columnMap["lastupdated"]])
	}
	return nil
}
