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
	"runtime"
	"sort"
	"time"

	"github.com/stockparfait/errors"
	"github.com/stockparfait/parallel"
	"github.com/stockparfait/stockparfait/message"

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
	MAD() float64 // mean absolute deviation
	Variance() float64
	CDF(x float64) float64 // returns max. quantile for x
	Copy() Distribution    // shallow-copy with a new instance of rand.Source
	// Set random seed when applicable. Mostly used in tests.
	Seed(uint64)
}

type DistributionWithHistogram interface {
	Distribution
	Histogram() *Histogram
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

func (d *StudentsT) Mean() float64 {
	return d.Mu
}

func (d *StudentsT) MAD() float64 {
	return d.Sigma * studentsTMAD(d.Nu)
}

func (d *StudentsT) Copy() Distribution {
	return &StudentsT{distuv.StudentsT{
		Mu:    d.Mu,
		Sigma: d.Sigma,
		Nu:    d.Nu,
		Src:   rand.NewSource(d.Src.Uint64()),
	}}
}

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

func (d *Normal) Mean() float64 {
	return d.Mu
}

func (d *Normal) MAD() float64 {
	return d.Sigma * normalMAD
}

func (d *Normal) Copy() Distribution {
	return &Normal{distuv.Normal{
		Mu:    d.Mu,
		Sigma: d.Sigma,
		Src:   rand.NewSource(d.Src.Uint64()),
	}}
}

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

var _ DistributionWithHistogram = &SampleDistribution{}

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

func (d *SampleDistribution) Quantile(x float64) float64 {
	return d.sample.Data()[d.quantileIndex(x)]
}

func (d *SampleDistribution) Prob(x float64) float64 {
	return d.Histogram().Prob(x)
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

func (d *SampleDistribution) Mean() float64 {
	return d.Sample().Mean()
}

func (d *SampleDistribution) MAD() float64 {
	return d.Sample().MAD()
}

func (d *SampleDistribution) Variance() float64 {
	return d.Sample().Variance()
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

func (d *SampleDistribution) Copy() Distribution {
	return &SampleDistribution{
		sample:    d.sample,
		rand:      rand.New(rand.NewSource(d.rand.Uint64())),
		buckets:   d.buckets,
		histogram: d.histogram,
	}
}

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

	if buckets.Auto && len(sample) >= 2 {
		buckets.FitTo(sample) // ignore the error, it preserves the value
	}
	return &SampleDistribution{
		sample:  NewSample().Init(sample),
		buckets: buckets,
		rand:    rand.New(rand.NewSource(uint64(time.Now().UnixNano()))),
	}
}

// NewSampleDistributionFromRand creates an instance of a SampleDistribution by
// sampling a given distribution. It requires Buckets to create a Histogram for
// computing a reasonable p.d.f.
func NewSampleDistributionFromRand(d Distribution, samples int, buckets *Buckets) *SampleDistribution {
	sample := make([]float64, samples)
	for i := 0; i < samples; i++ {
		sample[i] = d.Rand()
	}
	return NewSampleDistribution(sample, buckets)
}

// NewSampleDistributionFromRandDist is similar to NewSampleDistributionFromRand
// except that it uses fast stateful sample generation of RandDistribution.
func NewSampleDistributionFromRandDist[S any](d *RandDistribution[S], samples int, buckets *Buckets) *SampleDistribution {
	sample := make([]float64, samples)
	s := d.xform.InitState()
	for i := 0; i < samples; i++ {
		var x float64
		x, s = d.xform.Fn(d.source, s)
		sample[i] = x
	}
	return NewSampleDistribution(sample, buckets)
}

// ParallelSamplingConfig is a set of configuration parameters for
// RandDistribution suitable for use in user config file schema.
type ParallelSamplingConfig struct {
	BatchMin int     `json:"batch size min" default:"10"`
	BatchMax int     `json:"batch size max" default:"10000"`
	Samples  int     `json:"samples" default:"10000"` // for histogram
	Buckets  Buckets `json:"buckets"`
	// Biased sampling parameters, when applicable. Zero values indicate that the
	// caller must set appropriate defaults.
	Scale   float64 `json:"bias scale"` // size of uniform distribution area
	Power   float64 `json:"bias power"` // approach +-Inf near +-1 as 1/(1-t^(2*Power))
	Shift   float64 `json:"bias shift"` // value of x(t=0)
	Workers int     `json:"workers"`    // default: 2*runtime.NumCPU()
	Seed    int     `json:"seed"`       // for use in tests when > 0
}

var _ message.Message = &ParallelSamplingConfig{}

func (c *ParallelSamplingConfig) InitMessage(js any) error {
	if err := message.Init(c, js); err != nil {
		return errors.Annotate(err, "failed to init ParallelSamplingConfig")
	}
	if c.Workers <= 0 {
		c.Workers = 2 * runtime.NumCPU()
	}
	if c.BatchMin < 1 {
		return errors.Reason("batch size min=%d must be >= 1", c.BatchMin)
	}
	if c.BatchMax < c.BatchMin {
		return errors.Reason("batch size max=%d must be >= batch size min=%d",
			c.BatchMax, c.BatchMin)
	}
	return nil
}

// Transform is a stateful random variable transformer used by RandDistribution
// to generate its random values. The initial state generator and the transform
// function must be go routine safe.
//
// The random values Y_i are generated as Y_i, S_i = Fn(d, S_(i-1)), where
// S_0=InitState(). It is assumed that, asymptotically, generating multiple
// short sequences is statistically equivalent to generating a single long
// sequence.  If this property doesn't hold, the Y values likely cannot be
// directly modeled by a random variable.
//
// As an example, a sliding window compounding (the sum of last N d.Rand()
// values, or the log-profit over N steps) satisfies this property, but the
// unbounded sum (such as log-price) does not.
type Transform[State any] struct {
	InitState func() State
	Fn        func(d Distribution, state State) (float64, State)
}

// RandDistribution uses a transformed Rand method of a source distribution to
// create another distribution. In particular, its own Rand function simply
// calls the source's Rand and applies the transform. It estimates and caches
// mean, MAD and quantiles (as a histogram) from a set number of samples. It
// never stores the generated samples, so its memory footprint remains small.
type RandDistribution[State any] struct {
	context   context.Context
	config    *ParallelSamplingConfig
	source    Distribution // the source distribution
	xform     *Transform[State]
	histogram *Histogram
}

var _ DistributionWithHistogram = &RandDistribution[int]{}

// NewRandDistribution creates a Distribution using the transformation of the
// random sampler function of the source distribution. The source distribution
// is copied using Distribution.Copy method, and therefore can be sampled
// independently and in parallel with the original source. It uses the given
// number of samples to estimate and lazily cache mean, MAD and quantiles.
func NewRandDistribution[S any](ctx context.Context, source Distribution, xform *Transform[S], cfg *ParallelSamplingConfig) *RandDistribution[S] {
	if cfg == nil {
		cfg = &ParallelSamplingConfig{}
		if err := cfg.InitMessage(make(map[string]any)); err != nil {
			panic(errors.Annotate(err, "failed to init default config"))
		}
	}
	return &RandDistribution[S]{
		context: ctx,
		config:  cfg,
		source:  source.Copy(),
		xform:   xform,
	}
}

func (d *RandDistribution[S]) Rand() float64 {
	x, _ := d.xform.Fn(d.source, d.xform.InitState())
	return x
}

type randJobsIter[S any] struct {
	d *RandDistribution[S]
	i int
}

var _ parallel.JobsIter[*Histogram] = &randJobsIter[int]{}

func (r *randJobsIter[S]) Next() (parallel.Job[*Histogram], error) {
	c := r.d.config
	if r.i >= c.Samples {
		return nil, parallel.Done
	}
	batchSize := c.Samples / c.Workers
	if batchSize < c.BatchMin {
		batchSize = c.BatchMin
	}
	if batchSize > c.BatchMax {
		batchSize = c.BatchMax
	}
	if batchSize > c.Samples-r.i {
		batchSize = c.Samples - r.i
	}
	r.i += batchSize
	srcCopy := r.d.source.Copy()
	job := func() *Histogram {
		h := NewHistogram(&c.Buckets)
		xform := r.d.xform
		s := xform.InitState()
		for i := 0; i < batchSize; i++ {
			var x float64
			x, s = xform.Fn(srcCopy, s)
			h.Add(x)
		}
		return h
	}
	return job, nil
}

func (d *RandDistribution[S]) jobsIter() parallel.JobsIter[*Histogram] {
	return &randJobsIter[S]{
		d: d.Copy().(*RandDistribution[S]),
	}
}

// Histogram of the generator, lazily cached.
func (d *RandDistribution[S]) Histogram() *Histogram {
	// The method will panic if parallel jobs return unexpected results.
	if d.histogram == nil {
		d.histogram = NewHistogram(&d.config.Buckets)
		m := parallel.Map[*Histogram](d.context, d.config.Workers, d.jobsIter())
		for {
			h, err := m.Next()
			if err != nil { // can only be parallel.Done
				break
			}
			if err := d.histogram.AddHistogram(h); err != nil {
				panic(errors.Annotate(err, "failed to merge histogram"))
			}
		}
	}
	return d.histogram
}

func (d *RandDistribution[S]) Quantile(x float64) float64 {
	return d.Histogram().Quantile(x)
}

func (d *RandDistribution[S]) Prob(x float64) float64 {
	return d.Histogram().PDF(d.Histogram().Buckets().Bucket(x))
}

func (d *RandDistribution[S]) Mean() float64 {
	return d.Histogram().Mean()
}

func (d *RandDistribution[S]) MAD() float64 {
	return d.Histogram().MAD()
}

func (d *RandDistribution[S]) Variance() float64 {
	return d.Histogram().Variance()
}

func (d *RandDistribution[S]) CDF(x float64) float64 {
	return d.Histogram().CDF(x)
}

func (d *RandDistribution[S]) Copy() Distribution {
	return &RandDistribution[S]{
		context:   d.context,
		config:    d.config,
		source:    d.source.Copy(),
		xform:     d.xform,
		histogram: d.histogram,
	}
}

func (d *RandDistribution[S]) Seed(seed uint64) {
	d.source.Seed(seed)
}

// HistogramDistribution creates a Distribution out of a Histogram.
type HistogramDistribution struct {
	h    *Histogram
	rand *rand.Rand
}

var _ DistributionWithHistogram = &HistogramDistribution{}

// NewHistogramDistribution creates a new distribution out of h. Note, that h is
// stored as the original pointer, and not deep-copied. The caller must assure
// that h is not modified after creating this distribution, otherwise the
// behavior may be unpredictable.
func NewHistogramDistribution(h *Histogram) *HistogramDistribution {
	return &HistogramDistribution{
		h:    h,
		rand: rand.New(rand.NewSource(uint64(time.Now().UnixNano()))),
	}
}

func (d *HistogramDistribution) Rand() float64 {
	return d.h.Quantile(d.rand.Float64())
}

func (d *HistogramDistribution) Quantile(x float64) float64 {
	return d.h.Quantile(x)
}

func (d *HistogramDistribution) Prob(x float64) float64 {
	return d.h.Prob(x)
}

func (d *HistogramDistribution) CDF(x float64) float64 {
	return d.h.CDF(x)
}

func (d *HistogramDistribution) Mean() float64 {
	return d.h.Mean()
}

func (d *HistogramDistribution) MAD() float64 {
	return d.h.MAD()
}

func (d *HistogramDistribution) Variance() float64 {
	return d.h.Variance()
}

func (d *HistogramDistribution) Histogram() *Histogram {
	return d.h
}

// Copy shallow-copies the distribution. Note, that the underlying Histogram is
// copied by pointer, and not deep-copied.
func (d *HistogramDistribution) Copy() Distribution {
	return &HistogramDistribution{
		h:    d.h,
		rand: rand.New(rand.NewSource(d.rand.Uint64())),
	}
}

func (d *HistogramDistribution) Seed(seed uint64) {
	d.rand = rand.New(rand.NewSource(seed))
}

// CompoundRandDistribution creates a RandDistribution out of source compounded
// n times. That is, source.Rand() is invoked n times and the sum of its samples
// is a new single sample in the new distribution.
func CompoundRandDistribution(ctx context.Context, source Distribution, n int, cfg *ParallelSamplingConfig) *RandDistribution[struct{}] {
	xform := &Transform[struct{}]{
		InitState: func() struct{} { return struct{}{} },
		Fn: func(d Distribution, state struct{}) (float64, struct{}) {
			acc := 0.0
			for i := 0; i < n; i++ {
				acc += d.Rand()
			}
			return acc, struct{}{}
		},
	}
	return NewRandDistribution(ctx, source, xform, cfg)
}

// FastCompoundState is used in Transform by FastCompoundRandDistribution.
type FastCompoundState []float64

// FastCompoundRandDistribution creates a RandDistribution out of source
// compounded n times. However, the source.Rand() values are not recomputed n
// times for each new sample, but are taken as the sum of a sliding window in a
// single sequence of source samples. This reduces the number of generated
// source samples from N*numSamples to N+numSamples.  In practice, multiple such
// sequences are generated in parallel for further speedup.
func FastCompoundRandDistribution(ctx context.Context, source Distribution, n int, cfg *ParallelSamplingConfig) *RandDistribution[FastCompoundState] {
	xform := &Transform[FastCompoundState]{
		InitState: func() FastCompoundState { return FastCompoundState{} },
		Fn: func(d Distribution, state FastCompoundState) (float64, FastCompoundState) {
			if len(state) > 0 {
				state = state[1:]
			}
			for len(state) <= n {
				var last float64
				if len(state) > 0 {
					last = state[len(state)-1]
				}
				state = append(state, last+d.Rand())
			}
			return state[n] - state[0], state
		},
	}
	return NewRandDistribution(ctx, source, xform, cfg)
}

// CompoundSampleDistribution creates a SampleDistribution out of a random
// generator compounded n times. That is, `rnd` is invoked n times and the sum
// of its samples is a new single sample in the new distribution.
func CompoundSampleDistribution(ctx context.Context, source Distribution, n int, cfg *ParallelSamplingConfig) *SampleDistribution {
	d := CompoundRandDistribution(ctx, source, n, cfg)
	return NewSampleDistributionFromRand(d, cfg.Samples, &cfg.Buckets)
}

// FastCompoundSampleDistribution creates a SampleDistribution out of a random
// generator compounded n times. See FastCompoundRandDistribution.
func FastCompoundSampleDistribution(ctx context.Context, source Distribution, n int, cfg *ParallelSamplingConfig) *SampleDistribution {
	d := FastCompoundRandDistribution(ctx, source, n, cfg)
	return NewSampleDistributionFromRandDist(d, cfg.Samples, &cfg.Buckets)
}

type compHistJobsIter struct {
	c    *ParallelSamplingConfig
	d    Distribution
	n    int // compounding
	rand *rand.Rand
	i    int // samples counter
}

var _ parallel.JobsIter[*Histogram] = &compHistJobsIter{}

func (it *compHistJobsIter) Next() (parallel.Job[*Histogram], error) {
	c := it.c
	if it.i >= c.Samples {
		return nil, parallel.Done
	}
	batchSize := c.Samples / c.Workers
	if batchSize < c.BatchMin {
		batchSize = c.BatchMin
	}
	if batchSize > c.BatchMax {
		batchSize = c.BatchMax
	}
	if batchSize > c.Samples-it.i {
		batchSize = c.Samples - it.i
	}
	it.i += batchSize
	randCopy := rand.New(rand.NewSource(it.rand.Uint64()))
	dCopy := it.d.Copy() // in case d.Prod(x) is not go-routine safe
	scale := c.Scale
	if scale == 0 {
		if c.Buckets.N >= 2 { // ignore the extreme bucket ranges
			scale = math.Abs(c.Buckets.Bounds[c.Buckets.N-1])
			if l := math.Abs(c.Buckets.Bounds[1]); scale < l {
				scale = l
			}
		} else {
			scale = math.Abs(c.Buckets.Bounds[c.Buckets.N])
		}
		scale /= math.Sqrt(float64(it.n))
	}
	power := c.Power
	if power == 0 {
		power = math.Ceil(math.Sqrt(float64(it.n)))
	}
	job := func() *Histogram {
		h := NewHistogram(&c.Buckets)
		for i := 0; i < batchSize; i++ {
			var w float64 = 1
			var y float64 // sum of n source samples
			for j := 0; j < it.n; j++ {
				t := 2*randCopy.Float64() - 1
				for t == -1 { // exclude -1
					t = 2*randCopy.Float64() - 1
				}
				x := VarSubst(t, scale, power, c.Shift)
				w *= dCopy.Prob(x) * VarPrime(t, scale, power)
				y += x
			}
			h.AddWithWeight(y, w)
		}
		return h
	}
	return job, nil
}

// CompoundHistogram computes a histogram of an n-compounded source distribution
// from its p.d.f. source.Prob(x) method.
func CompoundHistogram(ctx context.Context, source Distribution, n int, c *ParallelSamplingConfig) *Histogram {

	it := &compHistJobsIter{
		c:    c,
		d:    source,
		n:    n,
		rand: rand.New(rand.NewSource(uint64(time.Now().UnixNano()))),
	}
	if c.Seed > 0 {
		it.rand = rand.New(rand.NewSource(uint64(c.Seed)))
	}
	h := NewHistogram(&c.Buckets)
	m := parallel.Map[*Histogram](ctx, c.Workers, it)
	for {
		hj, err := m.Next()
		if err != nil { // can only be parallel.Done
			break
		}
		if err := h.AddHistogram(hj); err != nil {
			panic(errors.Annotate(err, "failed to merge histogram"))
		}
	}
	return h
}
