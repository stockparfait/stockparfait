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
	"testing"

	"github.com/stockparfait/testutil"

	. "github.com/smartystreets/goconvey/convey"
)

func TestSpacingType(t *testing.T) {
	t.Parallel()

	Convey("SpacingType.InitMessage works", t, func() {
		var s SpacingType

		Convey("Default value", func() {
			So(s.InitMessage(testutil.JSON(`{}`)), ShouldBeNil)
			So(s, ShouldEqual, LinearSpacing)
		})

		Convey("linear", func() {
			So(s.InitMessage(testutil.JSON(`"linear"`)), ShouldBeNil)
			So(s, ShouldEqual, LinearSpacing)
			So(s.String(), ShouldEqual, "linear")
		})

		Convey("exponential", func() {
			So(s.InitMessage(testutil.JSON(`"exponential"`)), ShouldBeNil)
			So(s, ShouldEqual, ExponentialSpacing)
			So(s.String(), ShouldEqual, "exponential")
		})

		Convey("symmetric exponential", func() {
			So(s.InitMessage(testutil.JSON(`"symmetric exponential"`)), ShouldBeNil)
			So(s, ShouldEqual, SymmetricExponentialSpacing)
			So(s.String(), ShouldEqual, "symmetric exponential")
		})
	})
}

func TestBuckets(t *testing.T) {
	t.Parallel()

	rs := func(x []float64) []float64 { return testutil.RoundSlice(x, 5) }

	Convey("Buckets work", t, func() {
		Convey("linear spacing", func() {
			b, err := NewBuckets(10, 0.0, 10.0, LinearSpacing)
			So(err, ShouldBeNil)
			So(b.Bounds, ShouldResemble, []float64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
			So(b.X(5, 0.5), ShouldEqual, 5.5)
			So(b.Bucket(-0.1), ShouldEqual, 0)
			So(b.Bucket(3.2), ShouldEqual, 3)
			So(b.Bucket(9.5), ShouldEqual, 9)
			So(b.Bucket(10.6), ShouldEqual, 9)
			So(b.Size(0), ShouldEqual, 1.0)
			So(b.Size(9), ShouldEqual, 1.0)
			So(b.Size(-1), ShouldEqual, 0.0)
			So(b.Size(10), ShouldEqual, 0.0)
			So(b.Xs(0.5), ShouldResemble, []float64{
				0.5, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5})
			So(b.Xs(0.0), ShouldResemble, []float64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
			So(b.String(), ShouldEqual, "Buckets{N: 10, Spacing: linear, Min: 0, Max: 10}")
		})

		Convey("exponential spacing", func() {
			b, err := NewBuckets(6, 0.001, 1000, ExponentialSpacing)
			So(err, ShouldBeNil)
			So(rs(b.Bounds), ShouldResemble, []float64{
				0.001, 0.01, 0.1, 1.0, 10.0, 100.0, 1000.0})
			So(b.Bucket(0.0011), ShouldEqual, 0)
			So(b.Bucket(0.11), ShouldEqual, 2)
			So(b.Bucket(399.0), ShouldEqual, 5)
			So(testutil.Round(b.Size(0), 5), ShouldEqual, 0.009)
			So(testutil.Round(b.Size(5), 3), ShouldEqual, 900.0)
			So(rs(b.Xs(0.5)), ShouldResemble, []float64{
				0.0031623, 0.031623, 0.3162, 3.1623, 31.623, 316.23})
			So(rs(b.Xs(0.0)), ShouldResemble, []float64{
				0.001, 0.01, 0.1, 1.0, 10.0, 100.0})
		})

		Convey("symmetric exponential spacing", func() {
			b, err := NewBuckets(9, 0.01, 100.0, SymmetricExponentialSpacing)
			So(err, ShouldBeNil)
			So(rs(b.Bounds), ShouldResemble, []float64{
				-100.0, -10.0, -1.0, -0.1, -0.01, 0.01, 0.1, 1.0, 10.0, 100.0})
			So(b.Bucket(-200.0), ShouldEqual, 0)
			So(b.Bucket(-5.0), ShouldEqual, 1)
			So(b.Bucket(-1.0), ShouldEqual, 2)
			So(b.Bucket(-0.011), ShouldEqual, 3)
			So(b.Bucket(-0.01), ShouldEqual, 4)
			So(b.Bucket(0.0), ShouldEqual, 4)
			So(b.Bucket(0.01), ShouldEqual, 5)
			So(b.Bucket(0.02), ShouldEqual, 5)
			So(b.Bucket(0.1), ShouldEqual, 6)
			So(b.Bucket(1.0), ShouldEqual, 7)
			So(b.Bucket(10.0), ShouldEqual, 8)
			So(b.Bucket(20.0), ShouldEqual, 8)
			So(b.Size(0), ShouldEqual, 90.0)
			So(b.Size(8), ShouldEqual, 90.0)
			So(b.Size(4), ShouldEqual, 0.02)
			So(rs(b.Xs(0.5)), ShouldResemble, []float64{
				-31.623, -3.1623, -0.3162, -0.031623, 0,
				0.031623, 0.3162, 3.1623, 31.623})
			So(rs(b.Xs(0.0)), ShouldResemble, []float64{
				-100.0, -10.0, -1.0, -0.1, -0.01, 0.01, 0.1, 1.0, 10.0})
		})

		Convey("defaults with InitMessage", func() {
			var b Buckets
			So(b.InitMessage(testutil.JSON(`{}`)), ShouldBeNil)
			So(len(b.Bounds), ShouldEqual, 102)
			So(testutil.RoundFixed(b.X(50, 0.5), 5), ShouldEqual, 0.0)
			So(b.Auto, ShouldBeTrue)
		})

		Convey("FitTo works", func() {
			Convey("linear spacing", func() {
				b, err := NewBuckets(5, 1, 2, LinearSpacing)
				So(err, ShouldBeNil)
				data := []float64{-2.0, 0.0, 4.0}
				So(b.FitTo(data), ShouldBeNil)
				So(b.Min, ShouldEqual, -2.0)
				So(b.Max, ShouldEqual, 4.0)
			})

			Convey("exponential spacing for positive samples", func() {
				b, err := NewBuckets(5, 1, 2, ExponentialSpacing)
				So(err, ShouldBeNil)
				data := []float64{0.0, 0.2, 4.0, 10.0}
				So(b.FitTo(data), ShouldBeNil)
				So(b.Spacing, ShouldEqual, ExponentialSpacing)
				So(b.Min, ShouldEqual, 0.2)
				So(b.Max, ShouldEqual, 10.0)
			})

			Convey("exponential spacing for negative samples", func() {
				b, err := NewBuckets(5, 1, 2, ExponentialSpacing)
				So(err, ShouldBeNil)
				data := []float64{-3.0, 0.0, 0.2, 4.0, 10.0}
				So(b.FitTo(data), ShouldBeNil)
				So(b.Spacing, ShouldEqual, LinearSpacing)
				So(b.Min, ShouldEqual, -3.0)
				So(b.Max, ShouldEqual, 10.0)
			})

			Convey("symmetric exponential spacing for only positive samples", func() {
				b, err := NewBuckets(5, 1, 2, SymmetricExponentialSpacing)
				So(err, ShouldBeNil)
				data := []float64{0.0, 0.2, 4.0, 10.0}
				So(b.FitTo(data), ShouldBeNil)
				So(b.Spacing, ShouldEqual, ExponentialSpacing)
				So(b.Min, ShouldEqual, 0.2)
				So(b.Max, ShouldEqual, 10.0)
			})

			Convey("symmetric exponential spacing for full range", func() {
				b, err := NewBuckets(5, 1, 2, SymmetricExponentialSpacing)
				So(err, ShouldBeNil)
				data := []float64{-10.0, -3.0, -0.2, 0.0, 0.02, 4.0, 9.0}
				So(b.FitTo(data), ShouldBeNil)
				So(b.Spacing, ShouldEqual, SymmetricExponentialSpacing)
				So(b.Min, ShouldEqual, 0.02)
				So(b.Max, ShouldEqual, 10.0)
			})
		})
	})
}

func TestHistogram(t *testing.T) {
	t.Parallel()

	Convey("Histogram works", t, func() {
		Convey("with linear buckets", func() {
			b, err := NewBuckets(12, -200.0, 1000.0, LinearSpacing)
			So(err, ShouldBeNil)
			h := NewHistogram(b)
			for i := 0; i < 1000; i++ {
				h.Add(float64(i))
			}
			So(h.WeightsTotal(), ShouldEqual, 1000)
			So(h.Buckets().N, ShouldEqual, 12)
			So(h.Counts(), ShouldResemble, []uint{
				0, 0, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100})
			So(h.Weights(), ShouldResemble, []float64{
				0, 0, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100})
			So(h.Count(5), ShouldEqual, 100)
			So(h.Count(13), ShouldEqual, 0)
			So(h.Sums(), ShouldResemble, []float64{
				0, 0, 4950, 14950, 24950, 34950, 44950, 54950, 64950, 74950, 84950, 94950})
			So(h.Sum(7), ShouldEqual, 54950.0)
			So(h.Sum(13), ShouldEqual, 0.0)
			So(h.Xs(), ShouldResemble, []float64{
				-150, -50, 49.5, 149.5, 249.5, 349.5, 449.5, 549.5, 649.5, 749.5, 849.5, 949.5})
			So(h.PDFs(), ShouldResemble, []float64{
				0, 0, 0.001, 0.001, 0.001, 0.001, 0.001, 0.001, 0.001, 0.001, 0.001, 0.001})
			So(testutil.RoundFixedSlice(h.StdErrors(), 10), ShouldResemble, []float64{0, 0, 0.0028033796, 0.0012032171, 0.0009130649, 0.0007912784, 0.0007109822, 0.0006394433, 0.0005624354, 0.0004729673, 0.0003607038, 0.00018517})
			So(h.Mean(), ShouldEqual, 499.5)                     // actual: 499.5
			So(h.MAD(), ShouldEqual, 250.0)                      // actual: 250.0
			So(testutil.Round(h.Sigma(), 3), ShouldEqual, 287.0) // actual: ~288.7
			So(testutil.Round(h.Quantile(0.0), 5), ShouldEqual, 0.0)
			So(testutil.Round(h.Quantile(0.25), 5), ShouldEqual, 250.0)
			So(testutil.Round(h.Quantile(0.5), 5), ShouldEqual, 500.0)
			So(testutil.Round(h.Quantile(0.88), 5), ShouldEqual, 880.0)
			So(testutil.Round(h.Quantile(1.0), 5), ShouldEqual, 1000.0)
			So(h.CDF(-1.0), ShouldEqual, 0.0)
			So(h.CDF(0.0), ShouldEqual, 0.0)
			So(h.CDF(500.0), ShouldEqual, 0.5)
			So(h.CDF(550.0), ShouldEqual, 0.55)
			So(h.CDF(950.0), ShouldEqual, 0.95)
			So(h.CDF(1000.0), ShouldEqual, 1.0)
			So(h.CDF(1001.0), ShouldEqual, 1.0)

			Convey("from counts", func() {
				h2 := NewHistogram(b)
				So(h2.AddWeights([]float64{
					0, 0, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1}), ShouldBeNil)
				So(h.PDF(5), ShouldEqual, 0.001)
			})

			Convey("from another histogram", func() {
				h2 := NewHistogram(b)
				So(h2.AddHistogram(h), ShouldBeNil)
				So(h2.Counts(), ShouldResemble, h.Counts())
				So(h2.Weights(), ShouldResemble, h.Weights())
				So(h2.Sums(), ShouldResemble, h.Sums())
				So(h2.WeightsTotal(), ShouldEqual, h.WeightsTotal())
				So(h2.SumTotal(), ShouldEqual, h.SumTotal())
			})
		})

		Convey("with exponential buckets", func() {
			b, err := NewBuckets(100, 1.0, 1000.0, ExponentialSpacing)
			So(err, ShouldBeNil)
			h := NewHistogram(b)
			for i := 0; i < 1000; i++ {
				h.Add(float64(i))
			}
			So(h.WeightsTotal(), ShouldEqual, 1000)
			So(h.Buckets().N, ShouldEqual, 100)
			So(len(h.Counts()), ShouldEqual, 100)
			So(testutil.Round(h.Mean(), 5), ShouldEqual, 499.5)  // actual: 499.5
			So(testutil.Round(h.MAD(), 3), ShouldEqual, 250.0)   // acutal: 250.0
			So(testutil.Round(h.Sigma(), 3), ShouldEqual, 288.0) // actual: ~288.7
			So(testutil.Round(h.Quantile(0.0), 5), ShouldEqual, 1.0)
			So(testutil.Round(h.Quantile(0.25), 5), ShouldEqual, 249.16)
			So(testutil.Round(h.Quantile(0.5), 5), ShouldEqual, 499.15)
			So(testutil.Round(h.Quantile(0.88), 5), ShouldEqual, 879.6)
			So(testutil.Round(h.Quantile(1.0), 5), ShouldEqual, 1000.0)

			So(h.CDF(0.0), ShouldEqual, 0.0)
			So(h.CDF(1.0), ShouldEqual, 0.0)
			So(testutil.Round(h.CDF(500.0), 4), ShouldEqual, 0.501)
			So(testutil.Round(h.CDF(550.0), 4), ShouldEqual, 0.551)
			So(testutil.Round(h.CDF(950.0), 4), ShouldEqual, 0.951)
			So(h.CDF(1000.0), ShouldEqual, 1.0)
			So(h.CDF(1001.0), ShouldEqual, 1.0)

			Convey("PDF integrates to 1.0", func() {
				sum := 0.0
				for i, f := range h.PDFs() {
					sum += f * b.Size(i)
				}
				So(testutil.Round(sum, 5), ShouldEqual, 1.0)
			})
		})

		Convey("with symmetric exponential buckets and weights", func() {
			b, err := NewBuckets(9, 0.01, 100.0, SymmetricExponentialSpacing)
			So(err, ShouldBeNil)
			h := NewHistogram(b)
			for i := -100; i < 100; i++ {
				if i <= -90 { // test that weights add up correctly
					h.AddWithWeight(float64(i), 0.07)
					h.AddWithWeight(float64(i), 0.03)
					continue
				}
				h.AddWithWeight(float64(i), 0.1)
			}
			So(testutil.Round(h.WeightsTotal(), 6), ShouldEqual, 20.0)
			So(h.Buckets().N, ShouldEqual, 9)
			So(h.Buckets().Bounds, ShouldResemble, []float64{
				-100.0, -10.0, -1.0, -0.1, -0.01, 0.01, 0.1, 1.0, 10.0, 100.0})
			So(h.Counts(), ShouldResemble, []uint{101, 9, 1, 0, 1, 0, 0, 9, 90})
			So(testutil.RoundSlice(h.Weights(), 6), ShouldResemble, []float64{
				9, 0.9, 0.1, 0, 0.1, 0, 0, 0.9, 9})
			So(testutil.RoundFixed(h.Mean(), 3), ShouldEqual, -0.5)       // actual: -0.5
			So(testutil.Round(h.MAD(), 4), ShouldEqual, 50.0)             // actual: 50.0
			So(testutil.Round(h.Sigma(), 4), ShouldEqual, 52.2)           // actual: 57.7
			So(testutil.Round(h.Quantile(0.0), 5), ShouldEqual, -100.0)   // actual: -100.0
			So(testutil.Round(h.Quantile(0.25), 5), ShouldEqual, -27.826) // actual: -50.0
			So(testutil.Round(h.Quantile(0.5), 5), ShouldEqual, -0.01)    // actual: 0.0
			So(testutil.Round(h.Quantile(0.88), 5), ShouldEqual, 54.117)  // actual: 76.0
			So(testutil.Round(h.Quantile(1.0), 5), ShouldEqual, 100.0)    // actual: 100.0

			So(h.CDF(-501.0), ShouldEqual, 0.0)
			So(h.CDF(-500.0), ShouldEqual, 0.0)
			So(testutil.Round(h.CDF(0.0), 3), ShouldEqual, 0.5)
			So(testutil.Round(h.CDF(80), 4), ShouldEqual, 0.9)
			So(h.CDF(100.0), ShouldEqual, 1.0)
			So(h.CDF(101.0), ShouldEqual, 1.0)
		})
	})
}
