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
	"sort"
	"time"

	"golang.org/x/exp/rand"
	"gonum.org/v1/gonum/mathext"
	"gonum.org/v1/gonum/stat/distuv"
)

// SafeLog is a "safe" natural logarithm, which for x <= 0 returns -Inf.
func SafeLog(x float64) float64 {
	if x <= 0 {
		return math.Inf(-1)
	}
	return math.Log(x)
}

// Distribution API for common operations.
type Distribution interface {
	distuv.Rander
	distuv.Quantiler
	Prob(float64) float64 // the p.d.f. value at x
	Mean() float64
	MAD() float64          // mean absolute deviation
	CDF(x float64) float64 // returns max. quantile for x
	Copy() Distribution    // shallow-copy with a new instance of rand.Source
	// Set random seed when applicable. Mostly used in tests.
	Seed(uint64)
}

// studentsTMAD computes the mean absolute deviation of the unscaled T
// distribution.
func studentsTMAD(alpha float64) float64 {
	return 2.0 * math.Sqrt(alpha) / ((alpha - 1) * mathext.Beta(alpha/2.0, 0.5))
}

// Mean absolute deviation of the unscaled normal distribution (sigma=1).
var normalMAD = math.Sqrt(2.0 / math.Pi)

// StudentsT distribution.
type StudentsT struct {
	distuv.StudentsT
}

var _ Distribution = &StudentsT{}

// Mean implements Distribution.
func (d *StudentsT) Mean() float64 {
	return d.Mu
}

// MAD implements Distribution.
func (d *StudentsT) MAD() float64 {
	return d.Sigma * studentsTMAD(d.Nu)
}

// Copy implements Distribution.
func (d *StudentsT) Copy() Distribution {
	return &StudentsT{distuv.StudentsT{
		Mu:    d.Mu,
		Sigma: d.Sigma,
		Nu:    d.Nu,
		Src:   rand.NewSource(d.Src.Uint64()),
	}}
}

// Seed implements Distribution.
func (d *StudentsT) Seed(seed uint64) {
	d.StudentsT.Src = rand.NewSource(seed)
}

// NewStudentsTDistribution creates an instance of a Student's T distribution
// scaled and shifted to have a given mean and MAD (mean absolute deviation).
func NewStudentsTDistribution(alpha, mean, MAD float64) *StudentsT {
	return &StudentsT{distuv.StudentsT{
		Mu:    mean,
		Sigma: MAD / studentsTMAD(alpha),
		Nu:    alpha,
		Src:   rand.NewSource(uint64(time.Now().UnixNano())),
	}}
}

// Normal distribution.
type Normal struct {
	distuv.Normal
}

var _ Distribution = &Normal{}

// Mean implements Distribution.
func (d *Normal) Mean() float64 {
	return d.Mu
}

// MAD implements Distribution.
func (d *Normal) MAD() float64 {
	return d.Sigma * normalMAD
}

// Copy implements Distribution.
func (d *Normal) Copy() Distribution {
	return &Normal{distuv.Normal{
		Mu:    d.Mu,
		Sigma: d.Sigma,
		Src:   rand.NewSource(d.Src.Uint64()),
	}}
}

// Seed implements Distribution.
func (d *Normal) Seed(seed uint64) {
	d.Normal.Src = rand.NewSource(seed)
}

// NewNormalDistribution creates an instance of a normal distribution scaled and
// shifted for the given mean and MAD (mean absolute deviation).
func NewNormalDistribution(mean, MAD float64) *Normal {
	return &Normal{distuv.Normal{
		Mu:    mean,
		Sigma: MAD / normalMAD,
		Src:   rand.NewSource(uint64(time.Now().UnixNano())),
	}}
}

// SampleDistribution implements a distribution of a sample.
type SampleDistribution struct {
	sample    *Sample // assumes the samples are sorted in ascending order
	rand      *rand.Rand
	buckets   *Buckets
	histogram *Histogram // for a reasonable Prob / p.d.f.
}

var _ Distribution = &SampleDistribution{}

// Rand implements distuv.Rander.
func (d *SampleDistribution) Rand() float64 {
	return d.sample.Data()[d.rand.Intn(len(d.sample.Data()))]
}

// quantileIndex computes the index in the sample slice for the x's quantile.
func (d *SampleDistribution) quantileIndex(x float64) int {
	size := len(d.sample.Data())
	i := int(math.Floor(x * float64(size)))
	if i >= size {
		i = size - 1
	}
	if i < 0 {
		i = 0
	}
	return i
}

// Quantile implements distuv.Quantiler.
func (d *SampleDistribution) Quantile(x float64) float64 {
	return d.sample.Data()[d.quantileIndex(x)]
}

// Prob implements Distribution.
func (d *SampleDistribution) Prob(x float64) float64 {
	return d.Histogram().PDF(d.Histogram().Buckets().Bucket(x))
}

// CDF of the sample distribution.
func (d *SampleDistribution) CDF(x float64) float64 {
	s := d.sample.Data()
	// Binary search for the index after which the sample exceeds x.
	l := 0
	u := len(s) - 1
	if x < s[l] {
		return 0.0
	}
	if x >= s[u] {
		return 1.0
	}
	for l+1 < u {
		m := int((l + u) / 2)
		if x < s[m] {
			u = m
		} else {
			l = m
		}
	}
	return float64(l) / float64(len(s))
}

// Mean implements Distribution.
func (d *SampleDistribution) Mean() float64 {
	return d.Sample().Mean()
}

// MAD implements Distribution.
func (d *SampleDistribution) MAD() float64 {
	return d.Sample().MAD()
}

// Sample as the source of the distribution.
func (d *SampleDistribution) Sample() *Sample { return d.sample }

// Histogram of the sample distribution.
func (d *SampleDistribution) Histogram() *Histogram {
	if d.histogram == nil {
		d.histogram = NewHistogram(d.buckets)
		d.histogram.Add(d.sample.Data()...)
	}
	return d.histogram
}

// Copy implements Distribution.
func (d *SampleDistribution) Copy() Distribution {
	return &SampleDistribution{
		sample:    d.sample,
		rand:      rand.New(rand.NewSource(d.rand.Uint64())),
		buckets:   d.buckets,
		histogram: d.histogram,
	}
}

// Seed implements Distribution.
func (d *SampleDistribution) Seed(seed uint64) {
	d.rand = rand.New(rand.NewSource(seed))
}

// NewSampleDistribution creates an instance of a SampleDistribution. It
// requires Buckets to create a Histogram for computing a reasonable p.d.f.
// NOTE: it will sort the sample in place and store the slice as is, without
// deep copying. The caller is responsible for making a copy if the original
// order is important, or if the sample will later be modified by the caller.
func NewSampleDistribution(sample []float64, buckets *Buckets) *SampleDistribution {
	// Sort the sample for fast quantile and c.d.f. computation.
	sort.Slice(sample, func(i, j int) bool { return sample[i] < sample[j] })

	return &SampleDistribution{
		sample:  NewSample().Init(sample),
		buckets: buckets,
		rand:    rand.New(rand.NewSource(uint64(time.Now().UnixNano()))),
	}
}

// NewSampleDistributionFromRand creates an instance of a SampleDistribution by
// sampling a given distribution. It requires Buckets to create a Histogram for
// computing a reasonable p.d.f.
func NewSampleDistributionFromRand(ctx context.Context, d Distribution, samples int, buckets *Buckets) *SampleDistribution {
	sample := make([]float64, samples)
	for i := 0; i < samples; i++ {
		sample[i] = d.Rand()
	}
	return NewSampleDistribution(sample, buckets)
}

// RandDistribution uses a transformed Rand method of a source distribution to
// create another distribution. In particular, its own Rand function simply
// calls the source's Rand and applies the transform. It estimates and caches
// mean, MAD and quantiles (as a histogram) from a set number of samples. It
// never stores the generated samples, so its memory footprint remains small.
type RandDistribution struct {
	ctx       context.Context
	source    Distribution                 // the source distribution
	xform     func(d Distribution) float64 // new Rand based on d.Rand
	samples   int                          // number of samples to use for mean and histogram
	buckets   *Buckets
	mean      *float64
	mad       *float64
	histogram *Histogram
}

var _ Distribution = &RandDistribution{}

// NewRandDistribution creates a Distribution using the transformation of the
// random sampler function of the source distribution. The source distribution
// is copied using Distribution.Copy method, and therefore can be sampled
// independently and in parallel with the original source. It uses the given
// number of samples to estimate and lazily cache mean, MAD and quantiles.
func NewRandDistribution(ctx context.Context, source Distribution, xform func(d Distribution) float64, samples int, buckets *Buckets) *RandDistribution {
	return &RandDistribution{
		ctx:     ctx,
		source:  source.Copy(),
		xform:   xform,
		samples: samples,
		buckets: buckets,
	}
}

// Rand implements Distribution.
func (d *RandDistribution) Rand() float64 {
	return d.xform(d.source)
}

// Histogram of the generator, lazily cached.
func (d *RandDistribution) Histogram() *Histogram {
	if d.histogram == nil || d.mean == nil {
		d.histogram = NewHistogram(d.buckets)
		sum := 0.0
		for i := 0; i < d.samples; i++ {
			v := d.Rand()
			sum += v
			d.histogram.Add(v)
		}
		mean := sum / float64(d.samples)
		d.mean = &mean
	}
	return d.histogram
}

// Quantile implements Distribution.
func (d *RandDistribution) Quantile(x float64) float64 {
	return d.Histogram().Quantile(x)
}

// Prob implements Distribution.
func (d *RandDistribution) Prob(x float64) float64 {
	return d.Histogram().PDF(d.Histogram().Buckets().Bucket(x))
}

// Mean implements Distribution.
func (d *RandDistribution) Mean() float64 {
	d.Histogram() // compute and cache the mean
	return *d.mean
}

// MAD implement Distribution.
func (d *RandDistribution) MAD() float64 {
	if d.mad == nil {
		mean := d.Mean()
		sum := 0.0
		for i := 0; i < d.samples; i++ {
			v := d.Rand()
			sum += math.Abs(v - mean)
		}
		mad := sum / float64(d.samples)
		d.mad = &mad
	}
	return *d.mad
}

// CDF implements Distribution.
func (d *RandDistribution) CDF(x float64) float64 {
	return d.Histogram().CDF(x)
}

// Copy implements Distribution.
func (d *RandDistribution) Copy() Distribution {
	return &RandDistribution{
		ctx:       d.ctx,
		source:    d.source.Copy(),
		xform:     d.xform,
		samples:   d.samples,
		buckets:   d.buckets,
		mean:      d.mean,
		mad:       d.mad,
		histogram: d.histogram,
	}
}

// Seed implements Distribution.
func (d *RandDistribution) Seed(seed uint64) {
	d.source.Seed(seed)
}

// CompoundRandDistribution creates a RandDistribution out of source compounded
// n times. That is, source.Rand() is invoked n times and the sum of its samples
// is a new single sample in the new distribution.
func CompoundRandDistribution(ctx context.Context, source Distribution, n, samples int, buckets *Buckets) *RandDistribution {
	xform := func(d Distribution) float64 {
		acc := 0.0
		for i := 0; i < n; i++ {
			acc += d.Rand()
		}
		return acc
	}
	return NewRandDistribution(ctx, source, xform, samples, buckets)
}

// CompoundSampleDistribution creates a SampleDistribution out of a random
// generator compounded n times. That is, `rnd` is invoked n times and the sum
// of its samples is a new single sample in the new distribution.
func CompoundSampleDistribution(ctx context.Context, source Distribution, n, samples int, buckets *Buckets) *SampleDistribution {
	d := CompoundRandDistribution(ctx, source, n, samples, buckets)
	return NewSampleDistributionFromRand(ctx, d, samples, buckets)
}

// ExpectationMC computes a (potentially partial) expectation integral:
// \integral_{ low .. high } [ f(x) * d.Prob(x) * dx ] using the simple
// Monte-Carlo method of sampling f(x) with the given distribution sampler and
// computing the average. The bounds are inclusive. Note, that low may be -Inf,
// and high may be +Inf.
//
// The sampling stops either when the maxIter samples have been reached, or when
// the relative change in the result abs((res[k] - res[k-1])/res[k-1]) was less
// than precision for 100 iterations.
func ExpectationMC(ctx context.Context, f func(x float64) float64,
	d Distribution, low, high float64, maxIter int, precision float64) float64 {
	count := 0
	sum := 0.0
	iterSincePrecision := 0
	for i := 0; i < maxIter; i++ {
		x := d.Rand()
		prevRes := 0.0
		if count > 0 {
			prevRes = sum / float64(count)
		}
		count++
		if x < low || high < x {
			continue
		}
		sum += f(x)
		if count == 1 { // no prevRes yet
			continue
		}
		change := sum/float64(count) - prevRes
		if prevRes != 0.0 {
			change = change / prevRes
		}
		if change < precision {
			iterSincePrecision++
		} else {
			iterSincePrecision = 0
		}
		if iterSincePrecision >= 100 {
			break
		}
	}
	return sum / float64(count)
}
