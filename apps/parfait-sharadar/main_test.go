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

package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stockparfait/logging"

	. "github.com/smartystreets/goconvey/convey"
)

func TestMain(t *testing.T) {
	t.Parallel()

	tmpdir, tmpdirErr := os.MkdirTemp("", "test_sharadar")
	defer os.RemoveAll(tmpdir)

	Convey("Setup succeeded", t, func() {
		So(tmpdirErr, ShouldBeNil)
	})

	Convey("parseFlags", t, func() {
		flags, err := parseFlags([]string{
			"-cache", "path/to/cache", "-db", "name", "-log-level", "warning"})
		So(err, ShouldBeNil)
		So(flags.DBDir, ShouldEqual, "path/to/cache")
		So(flags.DBName, ShouldEqual, "name")
		So(flags.LogLevel, ShouldEqual, logging.Warning)
	})

	Convey("parseConfig", t, func() {
		fileName := filepath.Join(tmpdir, "config.toml")
		f, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
		So(err, ShouldBeNil)
		defer f.Close()

		_, err = f.Write([]byte(`key = "testKey"
tables = ["SEP", "SFP"]
`))
		So(err, ShouldBeNil)
		c, err := parseConfig(tmpdir)
		So(err, ShouldBeNil)
		So(c.Key, ShouldEqual, "testKey")
		So(c.Tables, ShouldResemble, []string{"SEP", "SFP"})
	})
}
