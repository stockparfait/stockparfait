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

	"github.com/stockparfait/parallel"
	"github.com/stockparfait/testutil"

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

		Convey("From Rand", func() {
			d := NewNormalDistribution(2.0, 3.0)
			d.Seed(seed)
			d2 := NewSampleDistributionFromRand(d, 1000, buckets)
			d2.Seed(seed)
			So(testutil.Round(d2.Mean(), 1), ShouldEqual, d.Mean())
			So(testutil.Round(d2.MAD(), 1), ShouldEqual, d.MAD())

			Convey("Compounded", func() {
				d.Seed(seed)
				ctx := context.Background()
				var cfg ParallelSamplingConfig
				So(cfg.InitMessage(testutil.JSON(`{}`)), ShouldBeNil)
				cfg.Samples = 5000 // less than that is not precise enough
				cfg.Buckets = *buckets
				cfg.Workers = 1

				Convey("direct compounding", func() {
					d2 := CompoundSampleDistribution(ctx, d, 16, &cfg)
					d2.Seed(seed)
					So(testutil.Round(d2.Mean(), 2), ShouldEqual, d.Mean()*16.0)
					So(testutil.Round(d2.MAD(), 2), ShouldEqual, d.MAD()*4.0)
					So(testutil.Round(d2.Variance(), 2), ShouldEqual,
						testutil.Round(16*d.Variance(), 2))
				})

				Convey("fast compounding", func() {
					d2 := FastCompoundSampleDistribution(ctx, d, 16, &cfg)
					d2.Seed(seed)
					So(testutil.Round(d2.Mean(), 2), ShouldEqual, 30)      // actual: 32
					So(testutil.Round(d2.MAD(), 2), ShouldEqual, 11)       // actual: 12
					So(testutil.Round(d2.Variance(), 2), ShouldEqual, 200) // actual: 230
				})
			})
		})

		Convey("With automatic bounds for buckets", func() {
			b := *buckets
			b.Auto = true
			d2 := NewSampleDistribution(data, &b)
			h := d2.Histogram()
			So(h.Buckets().Max, ShouldEqual, 6.0)
		})
	})

	Convey("RandDistribution works", t, func() {
		ctx := parallel.TestSerialize(context.Background())
		xform := &Transform{
			InitState: func() any { return nil },
			Fn: func(d Distribution, s any) (float64, any) {
				return d.Rand(), nil
			},
		}
		var cfg ParallelSamplingConfig
		js := testutil.JSON(`
{
  "samples": 1000,
  "workers": 1,
  "buckets": {
    "n": 4,
    "min": -2,
    "max": 2
  }
}`)
		So(cfg.InitMessage(js), ShouldBeNil)
		source := NewSampleDistribution(
			[]float64{-2.0, 0.0, 0.0, 2.0}, &cfg.Buckets)
		d := NewRandDistribution(ctx, source, xform, &cfg)
		d.Seed(seed)

		Convey("Copy works", func() { // must be called before d.Histogram()
			copy := d.Copy().(*RandDistribution)
			So(copy.Histogram().CountsTotal(), ShouldEqual, cfg.Samples)
		})

		Convey("Histogram used correct number of samples", func() {
			So(d.Histogram().CountsTotal(), ShouldEqual, cfg.Samples)
		})

		Convey("Quantile", func() {
			// Due to the wide [0..1) bucket, the 50th quantile is in its middle,
			// which is 0.5.
			So(testutil.RoundFixed(d.Quantile(0.5), 1), ShouldEqual, 0.5)
		})

		Convey("Prob", func() {
			So(testutil.Round(d.Prob(0.0), 2), ShouldEqual, 0.5)
		})

		Convey("Mean", func() {
			So(testutil.RoundFixed(d.Mean(), 0), ShouldEqual, 0.0)
		})

		Convey("MAD", func() {
			So(testutil.Round(d.MAD(), 2), ShouldEqual, 1.0)
		})

		Convey("Variance", func() {
			So(testutil.Round(d.Variance(), 1), ShouldEqual, 2.0)
		})

		Convey("CDF", func() {
			So(testutil.Round(d.CDF(0.5), 2), ShouldEqual, 0.5) // inverse of Quantile(0.5)
		})

		Convey("Compounded", func() {
			d := NewNormalDistribution(2.0, 3.0)
			d.Seed(seed)
			var compCfg ParallelSamplingConfig
			js := testutil.JSON(`
{
  "samples": 3000,
  "workers": 1,
  "buckets": {
    "n": 100,
    "min": -50,
    "max": 50
  }
}`)
			So(compCfg.InitMessage(js), ShouldBeNil)

			Convey("direct compounding", func() {
				d2 := CompoundRandDistribution(ctx, d, 16, &compCfg)
				d2.Seed(seed)
				So(testutil.Round(d2.Mean(), 2), ShouldEqual, d.Mean()*16.0)
				// Test MAD with up to 10% precision, hence the ratio.
				So(testutil.Round(d.MAD()*4.0/d2.MAD(), 2), ShouldEqual, 1.0)
			})

			Convey("fast compounding", func() {
				d2 := FastCompoundRandDistribution(ctx, d, 16, &compCfg)
				d2.Seed(seed)
				So(testutil.Round(d2.Mean(), 2), ShouldEqual, 30.0) // actual: 32
				// Test MAD with up to 10% precision, hence the ratio.
				So(testutil.Round(d.MAD()*4.0/d2.MAD(), 2), ShouldEqual, 1.0)
			})
		})

		Convey("with default config", func() {
			d2 := NewRandDistribution(ctx, source, xform, nil)
			So(d2.config.Workers, ShouldBeGreaterThanOrEqualTo, 1)
		})
	})

	Convey("HistogramDistribution works", t, func() {
		b, err := NewBuckets(10, 0.0, 1000.0, LinearSpacing)
		So(err, ShouldBeNil)
		h := NewHistogram(b)
		for i := 0; i < 1000; i++ {
			h.Add(float64(i))
		}
		d := NewHistogramDistribution(h)
		d.Seed(42)

		Convey("All methods work", func() {
			x := d.Rand()
			So(x, ShouldBeGreaterThanOrEqualTo, 0.0)
			So(x, ShouldBeLessThanOrEqualTo, 1000.0)
			So(d.Quantile(0.5), ShouldEqual, 500.0)
			So(d.Prob(100.0), ShouldEqual, 0.001)
			So(d.CDF(500.0), ShouldEqual, 0.5)
			So(d.Mean(), ShouldEqual, 499.5)
			So(d.MAD(), ShouldEqual, 250.0)
			So(testutil.Round(d.Variance(), 3), ShouldEqual, 82500.0) // actual: 83333.25
			So(d.Histogram(), ShouldEqual, h)
			copy, ok := d.Copy().(DistributionWithHistogram)
			So(ok, ShouldBeTrue)
			copy.Seed(42)
			So(copy.Rand(), ShouldEqual, x)
			copy.Seed(42) // test that rand.Source is not the same.
			So(d.Rand(), ShouldNotEqual, x)
		})

		Convey("Rand yields similar distribution", func() {
			h2 := NewHistogram(b)
			for i := 0; i < 1000; i++ {
				h2.Add(d.Rand())
			}
			So(testutil.Round(h2.Mean(), 2), ShouldEqual, 510.0) // actual: 499.5
			So(testutil.Round(h2.MAD(), 2), ShouldEqual, 250.0)
		})
	})

	Convey("CompoundHistogram works", t, func() {
		ctx := context.Background()
		cfgJSON := testutil.JSON(`
{
  "samples": 10000,
  "workers": 1,
  "buckets": {
    "n": 10,
    "min": -5,
    "max": 5
  },
  "seed": 42
}`)
		var cfg ParallelSamplingConfig
		So(cfg.InitMessage(cfgJSON), ShouldBeNil)
		n := 4
		d := NewNormalDistribution(0.0, 1.0/math.Sqrt(float64(n)))
		h := CompoundHistogram(ctx, d, n, &cfg)

		Convey("At least half the counts are useful", func() {
			So(h.Count(0)+h.Count(cfg.Buckets.N-1), ShouldBeLessThan, h.CountsTotal()/2)
		})

		Convey("Histogram's p.d.f. is approximately accurate", func() {
			dxN := NewNormalDistribution(0.0, 1.0) // true n-compounded normal
			// Ignore the extreme catch-all buckets.
			actual := make([]float64, cfg.Buckets.N-2)
			expected := make([]float64, cfg.Buckets.N-2)
			for i, x := range h.Xs() {
				if i == 0 || i == cfg.Buckets.N-1 {
					continue
				}
				actual[i-1] = math.Log(h.PDF(i))
				expected[i-1] = math.Log(dxN.Prob(x))
			}

			So(testutil.RoundSlice(actual, 1), ShouldResemble,
				testutil.RoundSlice(expected, 1))
			for i := 0; i < h.Buckets().N; i++ {
				So(h.stdErrs[i].n, ShouldBeGreaterThan, 0)
				So(h.StdError(i), ShouldBeGreaterThan, 0.0)
				So(h.StdError(i), ShouldBeLessThan, 0.1)
			}
		})
	})
}
