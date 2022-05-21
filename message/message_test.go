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
	"encoding/json"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func testJSON(js string) interface{} {
	var res interface{}
	if err := json.Unmarshal([]byte(js), &res); err != nil {
		return nil
	}
	return res
}

type Dog struct {
	Name       string  `json:"name" required:"true"`
	Breed      string  `json:"breed" default:"village dog"`
	Sex        string  `choices:"male,female" default:"female"`
	Age        float64 `default:"2.5"` // json:"Age" is assumed
	Legs       *int    `default:"4"`
	HasBone    bool    `default:"true"`
	Dead       bool
	Pups       []*Dog            `json:"pups,omitempty"`
	Tags       map[string]string `json:"tags"`
	Ignored    int               `json:"-"`
	unexported int
}

// Init implements Message.
func (d *Dog) Init(js interface{}) error {
	return Init(d, js)
}

type BadChoice struct {
	Choice string `choices:"foo,bar"` // no default
}

func (b *BadChoice) Init(js interface{}) error {
	return Init(b, js)
}

func TestMessage(t *testing.T) {
	t.Parallel()
	Convey("Init() works", t, func() {
		Convey("with required fields only", func() {
			var d Dog
			So(d.Init(testJSON(`{"name": "Doggy"}`)), ShouldBeNil)
			So(d.Name, ShouldEqual, "Doggy")
			So(d.Breed, ShouldEqual, "village dog")
			So(d.Age, ShouldEqual, 2.5)
			So(*d.Legs, ShouldEqual, 4)
			So(d.HasBone, ShouldBeTrue)
			So(d.Dead, ShouldBeFalse)
			So(len(d.Pups), ShouldEqual, 0)
		})

		Convey("with recursive Message entries", func() {
			var d Dog
			So(d.Init(testJSON(`{
        "name": "Mommy", "Legs": null, "HasBone": false, "Age": 5.2, "Dead": true,
        "tags": {"tag1": "foo", "tag2": "bar"},
        "pups": [
          {"name": "Bad Boy", "Age": 0.1, "Sex": "male"},
          {"name": "Good Girl", "Legs": 3}]
      }`)), ShouldBeNil)
			So(d.Name, ShouldEqual, "Mommy")
			So(d.Sex, ShouldEqual, "female")
			So(d.Legs, ShouldBeNil)
			So(d.HasBone, ShouldBeFalse)
			So(d.Age, ShouldEqual, 5.2)
			So(d.Dead, ShouldBeTrue)
			So(d.Tags, ShouldResemble, map[string]string{"tag1": "foo", "tag2": "bar"})
			So(len(d.Pups), ShouldEqual, 2)
			boy := d.Pups[0]
			girl := d.Pups[1]
			So(boy.Name, ShouldEqual, "Bad Boy")
			So(boy.Sex, ShouldEqual, "male")
			So(boy.Age, ShouldEqual, 0.1)
			So(*boy.Legs, ShouldEqual, 4)
			So(girl.Name, ShouldEqual, "Good Girl")
			So(girl.Sex, ShouldEqual, "female")
			So(girl.Age, ShouldEqual, 2.5)
			So(*girl.Legs, ShouldEqual, 3)
			So(d.unexported, ShouldEqual, 0)
		})

		Convey("with missing fields in recursive Init() call", func() {
			var d Dog
			// A pup is missing its name.
			So(d.Init(testJSON(`{"name": "Mommy", "pups": [{"Age": 0.1}]}`)), ShouldNotBeNil)
		})

		Convey("with ignored fields", func() {
			var d Dog
			err := d.Init(testJSON(`{"name": "D", "Ignored": 5}`))
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "unsupported fields for Dog: Ignored")
		})

		Convey("with unexported fields", func() {
			var d Dog
			err := d.Init(testJSON(`{"name": "D", "unexported": 5}`))
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "unsupported fields for Dog: unexported")
		})

		Convey("with incorrect sex", func() {
			var d Dog
			err := d.Init(testJSON(`{"name": "D", "Sex": "neutered"}`))
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring,
				"value for Sex is not in its choice list: 'neutered'")
		})

		Convey("with incorrect default choice", func() {
			var b BadChoice
			err := b.Init(testJSON(`{}`))
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring,
				"error setting Go zero value for Choice")
			So(err.Error(), ShouldContainSubstring,
				"value for Choice is not in its choice list: ''")
		})
	})

	Convey("StringIn works", t, func() {
		So(StringIn("dog", "cat", "dog", "mouse"), ShouldBeTrue)
		So(StringIn("bone", "cat", "dog"), ShouldBeFalse)
	})
}
