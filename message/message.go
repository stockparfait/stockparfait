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

package message

import (
	"reflect"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/stockparfait/errors"
)

// Message is the primitive building block of a JSON-based configuration or a
// communication protocol. It typically represents a JSON object, and is
// typically implemented by a struct holding the expected fields.
//
// It is intended to be implemented by struct pointers, e.g.:
//
//   type Dog struct {
//     Name string `json:"name" required:"true"`
//     Good bool `json:"good"`  // default false (the zero value)
//     Weight float64 `default:"25.5"` // json key is "Weight"
//     Sex string `required:"true" choices:"male,female"`
//     Ignored int `json:"-"`
//     Parent *Dog              // recursively parse Message
//     Pups []Dog `json:"pups"` // note the lack of pointer:
//                              // *Dog implements Message, Dog doesn't,
//                              // but it's still correctly populated.
//   }
//
//   func (d *Dog) InitMessage(js interface{}) error {
//     return message.Init(d, js)
//   }
type Message interface {
	// InitMessage converts a generic JSON read by the encoding/json package into
	// the specific message. In particular, this method typically checks for
	// required fields, sets the default values of optional fields, and makes sure
	// that no unrecognized fields are present.
	//
	// If a Message contains other Messages as fields, this method should be
	// called recursively on the nested Messages.
	InitMessage(js interface{}) error
}

// rMessage is the reflected Message type. Since it's an interface, we cannot
// obtain it directly, thus have to create a pointer to it (which is a non-nil
// reflect.Value even if its value is nil), and thus TypeOf returns a valid
// type.
var rMessage = reflect.TypeOf((*Message)(nil)).Elem()

func convertToMessage(jv interface{}, t reflect.Type) (reflect.Value, error) {
	var Nil reflect.Value
	if !t.Implements(rMessage) {
		return Nil, errors.Reason("type %s must implement Message", t.Name())
	}
	if t.Kind() != reflect.Ptr {
		return Nil, errors.Reason(
			"type %s implements Message but is not a pointer", t.Name())
	}
	ptr := reflect.New(t.Elem())
	err := ptr.MethodByName("InitMessage").Call(
		[]reflect.Value{reflect.ValueOf(jv)})[0].Interface()
	if err != nil {
		return Nil, errors.Annotate(err.(error), "%s.InitMessage() failed", t.Name())
	}
	return ptr, nil
}

// convertToType recursively converts raw JSON value to basic types, slices and
// map[string]* of the target type. Pointer types implementing Message are
// initialized with their InitMessage() method. If jv == nil, set to zero or
// default Message value, as appropriate.
func convertToType(jv interface{}, t reflect.Type) (reflect.Value, error) {
	var Nil reflect.Value
	if t.Implements(rMessage) {
		if jv == nil {
			return reflect.Zero(t), nil
		}
		ptr, err := convertToMessage(jv, t)
		if err != nil {
			return Nil, errors.Annotate(err, "failed to parse Message %s", t.Name())
		}
		return ptr, nil
	}
	if ptrTp := reflect.PtrTo(t); ptrTp.Implements(rMessage) {
		if jv == nil {
			jv = make(map[string]interface{}) // force default values for t
		}
		ptr, err := convertToMessage(jv, ptrTp)
		if err != nil {
			return Nil, errors.Annotate(err, "failed to parse Message %s", t.Name())
		}
		return reflect.Indirect(ptr), nil
	}
	if jv == nil {
		return reflect.Zero(t), nil
	}
	switch t.Kind() {
	case reflect.Ptr:
		v, err := convertToType(jv, t.Elem())
		if err != nil {
			return Nil, err
		}
		ptr := reflect.New(t.Elem())
		ptr.Elem().Set(v)
		return ptr, nil

	case reflect.Bool:
		v2, ok := jv.(bool)
		if !ok {
			return Nil, errors.Reason("not a bool type: %v", jv)
		}
		return reflect.ValueOf(v2), nil

	case reflect.Int:
		v2, ok := jv.(float64)
		if !ok {
			return Nil, errors.Reason("not a numeric type: %v", jv)
		}
		return reflect.ValueOf(int(v2)), nil

	case reflect.Float64:
		v2, ok := jv.(float64)
		if !ok {
			return Nil, errors.Reason("not a numeric type: %v", jv)
		}
		return reflect.ValueOf(v2), nil

	case reflect.String:
		v2, ok := jv.(string)
		if !ok {
			return Nil, errors.Reason("not a string type: %v", jv)
		}
		return reflect.ValueOf(v2), nil

	case reflect.Map:
		if t.Key().Kind() != reflect.String {
			return Nil, errors.Reason(
				"map[%s] is not supported", t.Key().Kind().String())
		}
		v2, ok := jv.(map[string]interface{})
		if !ok {
			return Nil, errors.Reason("not a map[string] type: %v", jv)
		}
		res := reflect.MakeMap(t)
		for k, v := range v2 {
			el, err := convertToType(v, t.Elem())
			if err != nil {
				return Nil, err
			}
			res.SetMapIndex(reflect.ValueOf(k), el)
		}
		return res, nil

	case reflect.Slice:
		v2, ok := jv.([]interface{})
		if !ok {
			return Nil, errors.Reason("not a slice type: %v", jv)
		}
		res := reflect.MakeSlice(t, len(v2), len(v2))
		for i, v := range v2 {
			el, err := convertToType(v, t.Elem())
			if err != nil {
				return Nil, err
			}
			res.Index(i).Set(el)
		}
		return res, nil

	default:
		return Nil, errors.Reason("unsupported type: %s", t.Name())
	}
}

// fromString attempts to convert a string s to the type t. This is used to
// extract default values from struct tags.
func fromString(s string, t reflect.Type) (reflect.Value, error) {
	var Nil reflect.Value
	switch t.Kind() {
	case reflect.Ptr:
		v, err := fromString(s, t.Elem())
		if err != nil {
			return Nil, err
		}
		ptr := reflect.New(t.Elem())
		ptr.Elem().Set(v)
		return ptr, nil
	case reflect.Bool:
		v, err := strconv.ParseBool(s)
		if err != nil {
			return Nil, errors.Annotate(err, "invalid bool value: %s", s)
		}
		return reflect.ValueOf(v), nil
	case reflect.Int:
		v, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return Nil, errors.Annotate(err, "invalid int value: %s", s)
		}
		return reflect.ValueOf(int(v)), nil
	case reflect.Float64:
		v, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return Nil, errors.Annotate(err, "invalid float64 value: %s", s)
		}
		return reflect.ValueOf(v), nil
	case reflect.String:
		return reflect.ValueOf(s), nil
	}
	return Nil, errors.Reason("type %s is not supported", t.Name())
}

// checkSet sets the value fv of a struct field f to the value v and checks that
// the value is valid.
func checkSet(f reflect.StructField, fv reflect.Value, v reflect.Value) error {
	if choices, ok := f.Tag.Lookup("choices"); ok {
		if f.Type.Kind() != reflect.String {
			return errors.Reason(
				"choices tag applied to a non-string field: %s", f.Name)
		}
		s, ok := v.Interface().(string)
		if !ok {
			return errors.Reason(
				"value for a string field %s is not a string", f.Name)
		}
		if !StringIn(s, strings.Split(choices, ",")...) {
			return errors.Reason(
				"value for %s is not in its choice list: '%s'", f.Name, s)
		}
	}
	fv.Set(v)
	return nil
}

// Init is a generic method to be used by most Message.Init implementations. It
// expects m to be a struct, and js to be a non-nil map[string]interface{}. It
// uses struct tags to know if a field is required or if it has a simple default
// value (such as a string, number or bool).
//
// If the field type is another Message, it calls the Message's Init()
// method. Otherwise, it converts whatever value it finds to the appropriate
// type and assigns it.
//
// It then checks the original JSON for any unrecognized fields and returns an
// error as appropriate.
//
// Recognized struct tags:
// `json:"field_name" required:"true" default:"value" choices:"one,two,three"`
//
// The `json:` tag is compatible with the encoding/json package. In particular,
// only exported fields are considered part of a message; a missing json tag is
// equivalent to `json:"FieldName"`, and qualifiers like `json:",omitempty"` are
// accepted but ignored. This allows the struct to be marshaled into a
// message-compatible JSON directly.
//
// The "choices" tag is currently supported only for string fields.
func Init(m Message, js interface{}) error {
	rt := reflect.TypeOf(m)
	if !(rt.Kind() == reflect.Ptr && rt.Elem().Kind() == reflect.Struct) {
		return errors.Reason(
			"expected Message instance to be a struct pointer, but got %s.",
			rt.Name())
	}
	if js == nil {
		return errors.Reason("JSON object is nil")
	}
	jsMap, ok := js.(map[string]interface{})
	if !ok {
		return errors.Reason("JSON object is not a map: %v.", js)
	}

	rt = rt.Elem() // we really need the original struct type and value
	rv := reflect.ValueOf(m).Elem()
	foundFields := make(map[string]struct{}) // to check for unknown fields
	missingRequired := []string{}
	for i := 0; i < rt.NumField(); i++ {
		f := rt.Field(i)
		rfv := rv.FieldByName(f.Name)
		firstChar, _ := utf8.DecodeRuneInString(f.Name)
		if !unicode.IsUpper(firstChar) {
			continue
		}
		jsonName := f.Name
		jsonTag := f.Tag.Get("json")
		if jsonTag != "" {
			parts := strings.Split(jsonTag, ",")
			if parts[0] == "-" {
				continue
			}
			if parts[0] != "" {
				jsonName = parts[0]
			}
		}
		if jv, ok := jsMap[jsonName]; ok {
			foundFields[jsonName] = struct{}{}
			v, err := convertToType(jv, f.Type)
			if err != nil {
				return errors.Annotate(err, "error assigning field %s", f.Name)
			}
			if err := checkSet(f, rfv, v); err != nil {
				return err
			}
			continue
		}

		// No value in JSON, figure out what to do.
		if f.Tag.Get("required") == "true" {
			missingRequired = append(missingRequired, jsonName)
			continue
		}
		if defaultVal, ok := f.Tag.Lookup("default"); ok {
			v, err := fromString(defaultVal, f.Type)
			if err != nil {
				return errors.Annotate(
					err, "error setting default value for %s", f.Name)
			}
			if err := checkSet(f, rfv, v); err != nil {
				return err
			}
			continue
		}
		// Not required and no default: set it to default or zero value. Note, that
		// we still need to check its validity, e.g. in case there is a `choices`
		// tag.
		v := reflect.Zero(f.Type)
		v, err := convertToType(nil, f.Type)
		if err != nil {
			return errors.Annotate(err, "error creating default value for %s", f.Name)
		}
		if err := checkSet(f, rfv, v); err != nil {
			return errors.Annotate(err, "error setting zero value for %s", f.Name)
		}
	}
	if len(missingRequired) != 0 {
		return errors.Reason(
			"missing required fields: %s",
			strings.Join(missingRequired, ", "))
	}
	extraFields := []string{}
	for k := range jsMap {
		if _, ok := foundFields[k]; ok {
			continue
		}
		extraFields = append(extraFields, k)
	}
	if len(extraFields) != 0 {
		return errors.Reason(
			"unsupported fields for %s: %s",
			rt.Name(), strings.Join(extraFields, ", "))
	}
	return nil
}

// StringIn checks that s equals one of the values.
func StringIn(s string, values ...string) bool {
	for _, v := range values {
		if s == v {
			return true
		}
	}
	return false
}
