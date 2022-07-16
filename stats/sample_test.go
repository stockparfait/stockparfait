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

package stats

import (
	"math"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestSample(t *testing.T) {
	t.Parallel()
	Convey("Sample works correctly", t, func() {
		s := NewSample().Init([]float64{1.5, 2.0, 2.5, 0.0})

		Convey("Data is correct", func() {
			So(s.Data(), ShouldResemble, []float64{1.5, 2.0, 2.5, 0.0})
		})

		Convey("Copy indeed copies data", func() {
			d := []float64{1.0, 2.0}
			s.Init(d) // baseline for reference
			s2 := NewSample().Copy(d)
			So(s.Data(), ShouldResemble, d)
			So(s2.Data(), ShouldResemble, d)

			d[1] = 3.0
			So(s.Data(), ShouldResemble, d)
			So(s2.Data(), ShouldResemble, []float64{1.0, 2.0})
		})

		Convey("Mean", func() {
			So(s.Mean(), ShouldEqual, 1.5)
			s.Init([]float64{2.0, 4.0})
			So(s.Mean(), ShouldEqual, 3.0)
			s.Init([]float64{})
			So(s.Mean(), ShouldEqual, 0.0)
		})

		Convey("MAD", func() {
			So(s.MAD(), ShouldEqual, 0.75)
			s.Init([]float64{2.0, 4.0})
			So(s.MAD(), ShouldEqual, 1.0)
			s.Init([]float64{})
			So(s.MAD(), ShouldEqual, 0.0)
		})

		Convey("Variance", func() {
			So(s.Variance(), ShouldEqual, 0.875)
			s.Init([]float64{2.0, 4.0})
			So(s.Variance(), ShouldEqual, 1.0)
			s.Init([]float64{})
			So(s.Variance(), ShouldEqual, 0.0)
		})

		Convey("Sigma", func() {
			So(s.Sigma(), ShouldEqual, math.Sqrt(0.875))
			s.Init([]float64{2.0, 4.0})
			So(s.Sigma(), ShouldEqual, 1.0)
			s.Init([]float64{})
			So(s.Sigma(), ShouldEqual, 0.0)
		})

		Convey("Normalize", func() {
			normalized, err := s.Normalize()
			So(err, ShouldBeNil)
			So(normalized.Data(), ShouldResemble, []float64{
				0.0, // == (1.5 - 1.5) / 0.75
				(2.0 - 1.5) / 0.75,
				(2.5 - 1.5) / 0.75,
				(0.0 - 1.5) / 0.75,
			})
			So(normalized.Mean(), ShouldEqual, 0.0)
			So(normalized.MAD(), ShouldEqual, 1.0)
		})
	})
}
