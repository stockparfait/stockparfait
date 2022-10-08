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
		So(PreciseEnough(3.1415, 0.0314, 0.01, true), ShouldBeTrue)
		So(PreciseEnough(3.1415, 0.0315, 0.01, true), ShouldBeFalse)
		So(PreciseEnough(0.31415, 0.011, 0.01, false), ShouldBeFalse)
		So(PreciseEnough(0.31415, 0.01, 0.011, false), ShouldBeTrue)
	})

	Convey("StandardError works", t, func() {
		Convey("Sequential accumulation is accurate", func() {
			// Test two sequences in different order.
			var stdErr, stdErr2 StandardError
			// Use a sequence of x from 1 to n.
			var sumSq float64
			n := 100
			mean := float64(n+1) / 2.0
			for i := 1; i <= n; i++ {
				x := float64(i)
				diff := x - mean
				sumSq += diff * diff
				stdErr.Add(x)
				stdErr2.Add(float64(n - i + 1))
			}
			variance := sumSq / float64(n)
			sigma := math.Sqrt(variance)
			So(stdErr.N(), ShouldEqual, uint(n))
			So(stdErr.Mean(), ShouldEqual, mean)
			So(testutil.Round(stdErr.Variance(), 7), ShouldEqual,
				testutil.Round(variance, 7))
			So(testutil.Round(stdErr.Sigma(), 7), ShouldEqual,
				testutil.Round(sigma, 7))
			So(stdErr2.N(), ShouldEqual, uint(n))
			So(stdErr2.Mean(), ShouldEqual, mean)
			So(testutil.Round(stdErr2.Variance(), 7), ShouldEqual,
				testutil.Round(variance, 7))
			So(testutil.Round(stdErr2.Sigma(), 7), ShouldEqual,
				testutil.Round(sigma, 7))
		})

		Convey("Zeros are added correctly", func() {
			// Test two sequences in different order.
			var stdErr, stdErr2 StandardError
			// Use a sequence of x from 1 to n followed by m zeros.
			var sumSq float64
			n := 100
			m := 50
			mean := float64(n*(n+1)) / 2.0 / float64(n+m)

			stdErr2.AddZeros(uint(m))
			for i := 1; i <= n; i++ {
				x := float64(i)
				diff := x - mean
				sumSq += diff * diff
				stdErr.Add(x)
				stdErr2.Add(float64(n - i + 1))
			}
			for i := 0; i < m; i++ {
				sumSq += mean * mean
			}
			stdErr.AddZeros(uint(m))
			variance := sumSq / float64(n+m)
			sigma := math.Sqrt(variance)
			So(stdErr.N(), ShouldEqual, uint(n+m))
			So(stdErr.Mean(), ShouldEqual, mean)
			So(testutil.Round(stdErr.Variance(), 7), ShouldEqual,
				testutil.Round(variance, 7))
			So(testutil.Round(stdErr.Sigma(), 7), ShouldEqual,
				testutil.Round(sigma, 7))
			So(stdErr2.N(), ShouldEqual, uint(n+m))
			So(stdErr2.Mean(), ShouldEqual, mean)
			So(testutil.Round(stdErr2.Variance(), 7), ShouldEqual,
				testutil.Round(variance, 7))
			So(testutil.Round(stdErr2.Sigma(), 7), ShouldEqual,
				testutil.Round(sigma, 7))
		})
	})

	Convey("ExpectationMC integration is accurate", t, func() {
		data := []float64{
			-5.0, -4.0, -3.0, -2.0, -1.0, 0.0, 1.0, 2.0, 3.0, 6.0}
		buckets, err := NewBuckets(5, -5.0, 5.0, LinearSpacing)
		So(err, ShouldBeNil)
		d := NewSampleDistribution(data, buckets)
		d.Seed(seed)

		Convey("on the full range", func() {
			var calls int
			one := func(x float64) float64 { calls++; return 1.0 }
			e := ExpectationMC(one, d.Rand, math.Inf(-1), math.Inf(1), 10, 1000, 0.01, true)
			So(testutil.Round(e, 2), ShouldEqual, 1.0)
			So(calls, ShouldBeGreaterThanOrEqualTo, 10)
		})

		Convey("on a subrange", func() {
			var calls int
			one := func(x float64) float64 { calls++; return 1.0 }
			e := ExpectationMC(one, d.Rand, -3.0, 3.0, 100, 1000, 0.01, true)
			So(testutil.Round(e, 2), ShouldEqual, 0.7)
			So(calls, ShouldBeGreaterThanOrEqualTo, 100)
		})
	})

	Convey("Variable substitution methods work", t, func() {
		Convey("VarSubst is symmetric and large near -1 and 1", func() {
			So(VarSubst(0, 5, 2, 0), ShouldEqual, 0)
			So(testutil.Round(VarSubst(0.5, 5, 2, 0), 3), ShouldEqual, 2.67)
			So(testutil.Round(VarSubst(-0.5, 5, 2, 0), 3), ShouldEqual, -2.67)
			So(testutil.Round(VarSubst(0.999, 5, 2, 10), 3), ShouldEqual, 1250.0+10.0)
			So(testutil.Round(VarSubst(-0.999, 5, 2, 10), 3), ShouldEqual, -1250.0+10.0)
		})

		Convey("VarPrime is indeed a derivative", func() {
			t := 0.5
			dt := 0.001
			r, b := 5.0, 2.0
			dx := VarSubst(t+dt/2.0, r, b, 0) - VarSubst(t-dt/2.0, r, b, 0)
			So(testutil.Round(dx/dt, 5), ShouldEqual,
				testutil.Round(VarPrime(t, r, b), 5))
		})

	})
}
