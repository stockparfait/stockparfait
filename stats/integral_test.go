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

	"github.com/stockparfait/testutil"

	. "github.com/smartystreets/goconvey/convey"
)

func TestIntegral(t *testing.T) {
	var seed uint64 = 42

	Convey("PreciseEnough works", t, func() {
		So(PreciseEnough(3.1415, 0.0314, 0.01), ShouldBeTrue)
		So(PreciseEnough(3.1415, 0.0315, 0.01), ShouldBeFalse)
		So(PreciseEnough(0.31415, 0.011, 0.01), ShouldBeFalse)
		So(PreciseEnough(0.31415, 0.01, 0.011), ShouldBeTrue)
	})

	Convey("ExpectationMC integration is accurate", t, func() {
		data := []float64{
			-5.0, -4.0, -3.0, -2.0, -1.0, 0.0, 1.0, 2.0, 3.0, 6.0}
		buckets, err := NewBuckets(5, -5.0, 5.0, LinearSpacing)
		So(err, ShouldBeNil)
		d := NewSampleDistribution(data, buckets)
		d.Seed(seed)

		Convey("on the full range", func() {
			one := func(x float64) float64 { return 1.0 }
			e := ExpectationMC(one, d.Rand, math.Inf(-1), math.Inf(1), 1000, 0.01)
			So(testutil.Round(e, 2), ShouldEqual, 1.0)
		})

		Convey("on a subrange", func() {
			var calls int
			one := func(x float64) float64 { calls++; return 1.0 }
			e := ExpectationMC(one, d.Rand, -3.0, 3.0, 1000, 0.01)
			So(testutil.Round(e, 2), ShouldEqual, 0.7)
			So(calls, ShouldBeGreaterThan, 1)
		})
	})

	Convey("Variable substitution methods work", t, func() {
		Convey("VarSubst covers is symmetric and large near -1 and 1", func() {
			So(VarSubst(0, 5, 2), ShouldEqual, 0)
			So(testutil.Round(VarSubst(0.5, 5, 2), 3), ShouldEqual, 2.67)
			So(testutil.Round(VarSubst(-0.5, 5, 2), 3), ShouldEqual, -2.67)
			So(testutil.Round(VarSubst(0.999, 5, 2), 3), ShouldEqual, 1250)
			So(testutil.Round(VarSubst(-0.999, 5, 2), 3), ShouldEqual, -1250)
		})

		Convey("VarPrime is indeed a derivative", func() {
			t := 0.5
			dt := 0.001
			r, b := 5.0, 2.0
			dx := VarSubst(t+dt/2.0, r, b) - VarSubst(t-dt/2.0, r, b)
			So(testutil.Round(dx/dt, 5), ShouldEqual,
				testutil.Round(VarPrime(t, r, b), 5))
		})

	})
}
