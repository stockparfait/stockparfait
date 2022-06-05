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
	"context"
	"math"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestDistribution(t *testing.T) {
	seed := uint64(42)

	Convey("SafeLog works correctly", t, func() {
		So(SafeLog(0.0), ShouldEqual, math.Inf(-1))
		So(math.Exp(SafeLog(0.0)), ShouldEqual, 0.0)
	})

	Convey("Can create T distribution", t, func() {
		d := NewStudentsTDistribution(3.0, 0.0, 1.0)
		d.Seed(seed)
		So(d.Quantile(0.5), ShouldEqual, 0.0)
	})

	Convey("Can create normal distribution", t, func() {
		d := NewNormalDistribution(0.0, 1.0)
		d.Seed(seed)
		So(d.Quantile(0.5), ShouldEqual, 0.0)
	})

	Convey("Sample distribution works", t, func() {
		ctx := context.Background()
		data := []float64{
			-5.0, -4.0, -3.0, -2.0, -1.0, 0.0, 1.0, 2.0, 3.0, 6.0}
		buckets, err := NewBuckets(5, -5.0, 5.0, LinearSpacing)
		So(err, ShouldBeNil)
		d := NewSampleDistribution(data, buckets)
		d.Seed(seed)

		Convey("Rand()", func() {
			x := d.Rand()
			So(x, ShouldBeGreaterThanOrEqualTo, -5.0)
			So(x, ShouldBeLessThanOrEqualTo, 6.0)
		})

		Convey("Quantile()", func() {
			So(d.Quantile(-0.1), ShouldEqual, -5.0) // out of range
			So(d.Quantile(0.5), ShouldEqual, 0.0)
			So(d.Quantile(1.1), ShouldEqual, 6.0) // out of range
		})

		Convey("Prob()", func() {
			So(d.Prob(0.0), ShouldEqual, 0.1) // 2/10 samples, bucket size = 2
		})

		Convey("CDF()", func() {
			So(d.CDF(-10.0), ShouldEqual, 0.0)
			So(d.CDF(0.0), ShouldEqual, 0.5)
			So(d.CDF(10.0), ShouldEqual, 1.0)
		})

		Convey("ExpectationMC is accurate on full range", func() {
			one := func(x float64) float64 { return 1.0 }
			e := ExpectationMC(ctx, one, d, math.Inf(-1), math.Inf(1), 1000, 0.01)
			So(round(e, 2), ShouldEqual, 1.0)
		})

		Convey("ExpectationMC is accurate on a subrange", func() {
			one := func(x float64) float64 { return 1.0 }
			e := ExpectationMC(ctx, one, d, -3.0, 3.0, 1000, 0.01)
			So(round(e, 2), ShouldEqual, 0.7)
		})

		Convey("From Rand", func() {
			d := NewNormalDistribution(2.0, 3.0)
			d.Seed(seed)
			d2 := NewSampleDistributionFromRand(ctx, d, 1000, buckets)
			d2.Seed(seed)
			So(round(d2.Mean(), 1), ShouldEqual, d.Mean())
			So(round(d2.MAD(), 1), ShouldEqual, d.MAD())

			Convey("Compounded", func() {
				d.Seed(seed)
				samples := 4000 // less than that is not precise enough
				d2 := CompoundSampleDistribution(ctx, d, 16, samples, buckets)
				d2.Seed(seed)
				So(round(d2.Mean(), 2), ShouldEqual, d.Mean()*16.0)
				So(round(d2.MAD(), 2), ShouldEqual, d.MAD()*4.0)
			})
		})
	})

	Convey("RandDistribution works", t, func() {
		ctx := context.Background()
		buckets, err := NewBuckets(4, -2.0, 2.0, LinearSpacing)
		So(err, ShouldBeNil)
		source := NewSampleDistribution(
			[]float64{-2.0, 0.0, 0.0, 2.0}, buckets)
		xform := func(d Distribution) float64 { return d.Rand() }
		d := NewRandDistribution(ctx, source, xform, 1000, buckets)
		d.Seed(seed)

		Convey("Quantile", func() {
			// Round 1+quantile, to keep only 1 decimal place. Due to the wide [0..1)
			// bucket, the 50th quantile is in its middle, which is 0.5.
			So(round(1.0+d.Quantile(0.5), 2), ShouldEqual, 1.5)
		})

		Convey("Prob", func() {
			So(round(d.Prob(0.0), 2), ShouldEqual, 0.5)
		})

		Convey("Mean", func() {
			So(round(1.0+d.Mean(), 2), ShouldEqual, 1.0)
		})

		Convey("MAD", func() {
			So(round(d.MAD(), 2), ShouldEqual, 1.0)
		})

		Convey("Compounded", func() {
			d := NewNormalDistribution(2.0, 3.0)
			d.Seed(seed)
			d2 := CompoundRandDistribution(ctx, d, 16, 2000, buckets)
			d2.Seed(seed)
			So(round(d2.Mean(), 2), ShouldEqual, d.Mean()*16.0)
			// Test MAD with up to 10% precision, hence the ratio.
			So(round(d.MAD()*4.0/d2.MAD(), 2), ShouldEqual, 1.0)
		})
	})
}
