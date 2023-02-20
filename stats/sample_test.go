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
		data := []float64{1.5, 2.0, 2.5, 0.0}

		Convey("Data is correct", func() {
			So(NewSample(data).Data(), ShouldResemble, data)
		})

		Convey("Copy indeed copies data", func() {
			d := []float64{1.0, 2.0}
			s := NewSample(d)
			s2 := s.Copy()
			So(s.Data(), ShouldResemble, d)
			So(s2.Data(), ShouldResemble, d)

			d[1] = 3.0
			So(s.Data(), ShouldResemble, d)
			So(s2.Data(), ShouldResemble, []float64{1.0, 2.0})
		})

		Convey("Mean", func() {
			So(NewSample(data).Mean(), ShouldEqual, 1.5)
			So(NewSample([]float64{2.0, 4.0}).Mean(), ShouldEqual, 3.0)
			So(NewSample([]float64{}).Mean(), ShouldEqual, 0.0)
		})

		Convey("MAD", func() {
			So(NewSample(data).MAD(), ShouldEqual, 0.75)
			So(NewSample([]float64{2.0, 4.0}).MAD(), ShouldEqual, 1.0)
			So(NewSample([]float64{}).MAD(), ShouldEqual, 0.0)
		})

		Convey("Variance", func() {
			So(NewSample(data).Variance(), ShouldEqual, 0.875)
			So(NewSample([]float64{2.0, 4.0}).Variance(), ShouldEqual, 1.0)
			So(NewSample([]float64{}).Variance(), ShouldEqual, 0.0)
		})

		Convey("Sigma", func() {
			So(NewSample(data).Sigma(), ShouldEqual, math.Sqrt(0.875))
			So(NewSample([]float64{2.0, 4.0}).Sigma(), ShouldEqual, 1.0)
			So(NewSample([]float64{}).Sigma(), ShouldEqual, 0.0)
		})

		Convey("Normalize", func() {
			normalized, err := NewSample(data).Normalize()
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
