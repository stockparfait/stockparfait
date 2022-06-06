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

	. "github.com/smartystreets/goconvey/convey"
)

func TestBuckets(t *testing.T) {
	t.Parallel()

	rs := func(x []float64) []float64 { return roundSlice(x, 5) }

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
		})

		Convey("exponential spacing", func() {
			b, err := NewBuckets(6, 0.001, 1000, ExponentialSpacing)
			So(err, ShouldBeNil)
			So(rs(b.Bounds), ShouldResemble, []float64{
				0.001, 0.01, 0.1, 1.0, 10.0, 100.0, 1000.0})
			So(b.Bucket(0.0011), ShouldEqual, 0)
			So(b.Bucket(0.11), ShouldEqual, 2)
			So(b.Bucket(399.0), ShouldEqual, 5)
			So(round(b.Size(0), 5), ShouldEqual, 0.009)
			So(round(b.Size(5), 3), ShouldEqual, 900.0)
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
	})
}

func TestHistogram(t *testing.T) {
	t.Parallel()

	Convey("Histogram works", t, func() {
		Convey("with linear buckets", func() {
			b, err := NewBuckets(10, 0.0, 1000.0, LinearSpacing)
			So(err, ShouldBeNil)
			h := NewHistogram(b)
			for i := 0; i < 1000; i++ {
				h.Add(float64(i))
			}
			So(h.Size(), ShouldEqual, 1000)
			So(h.Buckets().NumBuckets, ShouldEqual, 10)
			So(h.Counts(), ShouldResemble, []uint{
				100, 100, 100, 100, 100, 100, 100, 100, 100, 100})
			So(h.Count(5), ShouldEqual, 100)
			So(h.Count(11), ShouldEqual, 0)
			So(h.PDFs(), ShouldResemble, []float64{
				0.001, 0.001, 0.001, 0.001, 0.001, 0.001, 0.001, 0.001, 0.001, 0.001})
			So(h.Mean(), ShouldEqual, 500.0)
			So(round(h.Quantile(0.0), 5), ShouldEqual, 0.0)
			So(round(h.Quantile(0.25), 5), ShouldEqual, 250.0)
			So(round(h.Quantile(0.5), 5), ShouldEqual, 500.0)
			So(round(h.Quantile(0.88), 5), ShouldEqual, 880.0)
			So(round(h.Quantile(1.0), 5), ShouldEqual, 1000.0)
			So(h.CDF(-1.0), ShouldEqual, 0.0)
			So(h.CDF(0.0), ShouldEqual, 0.0)
			So(h.CDF(500.0), ShouldEqual, 0.5)
			So(h.CDF(550.0), ShouldEqual, 0.55)
			So(h.CDF(950.0), ShouldEqual, 0.95)
			So(h.CDF(1000.0), ShouldEqual, 1.0)
			So(h.CDF(1001.0), ShouldEqual, 1.0)

			Convey("from another histogram", func() {
				h2 := NewHistogram(b)
				So(h2.AddCounts(h.Counts()), ShouldBeNil)
				So(h2.Counts(), ShouldResemble, h.Counts())
			})
		})

		Convey("with exponential buckets", func() {
			b, err := NewBuckets(100, 1.0, 1000.0, ExponentialSpacing)
			So(err, ShouldBeNil)
			h := NewHistogram(b)
			for i := 0; i < 1000; i++ {
				h.Add(float64(i))
			}
			So(h.Size(), ShouldEqual, 1000)
			So(h.Buckets().NumBuckets, ShouldEqual, 100)
			So(len(h.Counts()), ShouldEqual, 100)
			So(round(h.Mean(), 5), ShouldEqual, 499.25)
			So(round(h.Quantile(0.0), 5), ShouldEqual, 1.0)
			So(round(h.Quantile(0.25), 5), ShouldEqual, 249.16)
			So(round(h.Quantile(0.5), 5), ShouldEqual, 499.15)
			So(round(h.Quantile(0.88), 5), ShouldEqual, 879.6)
			So(round(h.Quantile(1.0), 5), ShouldEqual, 1000.0)

			So(h.CDF(0.0), ShouldEqual, 0.0)
			So(h.CDF(1.0), ShouldEqual, 0.0)
			So(round(h.CDF(500.0), 4), ShouldEqual, 0.501)
			So(round(h.CDF(550.0), 4), ShouldEqual, 0.551)
			So(round(h.CDF(950.0), 4), ShouldEqual, 0.951)
			So(h.CDF(1000.0), ShouldEqual, 1.0)
			So(h.CDF(1001.0), ShouldEqual, 1.0)

			Convey("PDF integrates to 1.0", func() {
				sum := 0.0
				for i, f := range h.PDFs() {
					sum += f * b.Size(i)
				}
				So(round(sum, 5), ShouldEqual, 1.0)
			})
		})

		Convey("with symmetric exponential buckets", func() {
			b, err := NewBuckets(9, 0.01, 100.0, SymmetricExponentialSpacing)
			So(err, ShouldBeNil)
			h := NewHistogram(b)
			for i := -100; i < 100; i++ {
				h.Add(float64(i))
			}
			So(h.Size(), ShouldEqual, 200)
			So(h.Buckets().NumBuckets, ShouldEqual, 9)
			So(h.Buckets().Bounds, ShouldResemble, []float64{
				-100.0, -10.0, -1.0, -0.1, -0.01, 0.01, 0.1, 1.0, 10.0, 100.0})
			So(h.Counts(), ShouldResemble, []uint{
				90, 9, 1, 0, 1, 0, 0, 9, 90})
			So(roundFixed(h.Mean(), 2), ShouldEqual, 0.0)
			So(round(h.Quantile(0.0), 5), ShouldEqual, -100.0)   // actual: -100.0
			So(round(h.Quantile(0.25), 5), ShouldEqual, -27.826) // actual: -50.0
			So(round(h.Quantile(0.5), 5), ShouldEqual, -0.1)     // actual: 0.0
			So(round(h.Quantile(0.88), 5), ShouldEqual, 54.117)  // actual: 76.0
			So(round(h.Quantile(1.0), 5), ShouldEqual, 100.0)    // actual: 100.0

			So(h.CDF(-501.0), ShouldEqual, 0.0)
			So(h.CDF(-500.0), ShouldEqual, 0.0)
			So(round(h.CDF(0.0), 3), ShouldEqual, 0.5)
			So(round(h.CDF(80), 4), ShouldEqual, 0.9)
			So(h.CDF(100.0), ShouldEqual, 1.0)
			So(h.CDF(101.0), ShouldEqual, 1.0)
		})
	})
}
