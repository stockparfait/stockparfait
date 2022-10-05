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
	"fmt"
	"math"

	"github.com/stockparfait/errors"
	"github.com/stockparfait/stockparfait/message"
)

// SpacingType is enum for different ways buckets are spaced out.
type SpacingType uint8

var _ message.Message = (*SpacingType)(nil)

// Values of SpacingType:
// - LinearSpacing divides the interval into n equal parts.
//
// - ExponentialSpacing divides the log-space interval into n equal parts, thus
//   the buckets in the original interval grow exponentially away from
//   zero. Note, that Min must be > 0.
//
// - SymmetricExponentialSpacing makes the exponential spacing symmetric around
//   zero. That is, the buckets grow exponentially away from zero in both
//   directions, and the middle bucket spans [-Min..Min]. It requires n to be
//   odd, and Min > 0, but the actual interval is [-Max..Max].
const (
	LinearSpacing SpacingType = iota
	ExponentialSpacing
	SymmetricExponentialSpacing
)

func (s *SpacingType) InitMessage(js interface{}) error {
	switch v := js.(type) {
	case map[string]interface{}: // default value
		*s = LinearSpacing
	case string:
		switch v {
		case "linear":
			*s = LinearSpacing
		case "exponential":
			*s = ExponentialSpacing
		case "symmetric exponential":
			*s = SymmetricExponentialSpacing
		default:
			return errors.Reason("unsupported spacing '%s'", v)
		}
	default:
		return errors.Reason("unexpected JSON type: %T", js)
	}
	return nil
}

// String prints SpacingType. It's a value method, so it prints correctly in
// fmt.Printf.
func (s SpacingType) String() string {
	switch s {
	case LinearSpacing:
		return "linear"
	case ExponentialSpacing:
		return "exponential"
	case SymmetricExponentialSpacing:
		return "symmetric exponential"
	}
	return "invalid"
}

// Buckets configures the properties of histogram buckets. It implements
// message.Message, thus can be directly used in configs.
type Buckets struct {
	N int `json:"n" default:"101"`
	// Indicate that spacing / min / max can be set automatically.
	Auto    bool        `json:"auto bounds" default:"true"`
	Spacing SpacingType `json:"spacing"` // choices:"linear,exponential,symmetric exponential"
	Min     float64     `json:"min" default:"-50"`
	Max     float64     `json:"max" default:"50"`
	Bounds  []float64   `json:"-"` // n+1 bucket boundaries, auto-generated
}

var _ message.Message = &Buckets{}

// String prints Buckets. It is a value method, so non-pointer Buckets will
// print correctly in fmt.Printf.
func (b Buckets) String() string {
	return fmt.Sprintf("Buckets{N: %d, Spacing: %s, Min: %g, Max: %g}",
		b.N, b.Spacing, b.Min, b.Max)
}

func (b *Buckets) InitMessage(js interface{}) error {
	if err := message.Init(b, js); err != nil {
		return errors.Annotate(err, "failed to init Buckets")
	}
	if err := b.checkValues(); err != nil {
		return errors.Annotate(err, "invalid Buckets values")
	}
	b.setBounds()
	return nil
}

func (b *Buckets) checkValues() error {
	if b.Spacing > SymmetricExponentialSpacing {
		return errors.Reason("invalid spacing value: %d", b.Spacing)
	}
	if b.Min >= b.Max {
		return errors.Reason("invalid interval: minval=%f >= maxval=%f",
			b.Min, b.Max)
	}
	if b.N <= 0 {
		return errors.Reason("n=%d must be > 0", b.N)
	}
	if b.Spacing != LinearSpacing && b.Min <= 0.0 {
		return errors.Reason("minval=%f must be > 0 for non-linear spacing", b.Min)
	}
	if b.Spacing == SymmetricExponentialSpacing && !(b.N >= 3 && b.N%2 == 1) {
		return errors.Reason(
			"symmetric exponential spacing requires n=%d to be odd and >= 3", b.N)
	}
	return nil
}

// NewBuckets creates and initializes a new buckets object.
func NewBuckets(n int, minval, maxval float64, spacing SpacingType) (*Buckets, error) {
	b := &Buckets{}
	b.N = n
	b.Min = minval
	b.Max = maxval
	b.Spacing = spacing
	if err := b.checkValues(); err != nil {
		return nil, errors.Annotate(err, "invalid Buckets values")
	}
	b.setBounds()
	return b, nil
}

// SameAs checks if b defines the same buckets as b2.
func (b *Buckets) SameAs(b2 *Buckets) bool {
	return b.N == b2.N && b.Spacing == b2.Spacing && b.Min == b2.Min &&
		b.Max == b2.Max
}

// linearVal computes the value in the i'th linearly spaced bucket.
func linearVal(n, i int, shift, minval, maxval float64) float64 {
	stepSize := (maxval - minval) / float64(n)
	return minval + (float64(i)+shift)*stepSize
}

// expVal computes the value in the i'th exponentially spaced bucket.
func expVal(n, i int, shift, minval, maxval float64) float64 {
	return math.Pow(10.0, linearVal(
		n, i, shift, math.Log10(minval), math.Log10(maxval)))
}

// symmExpVal computes the value in the i'th symmetrically exponentially spaced
// bucket.
func symmExpVal(n, i int, shift, minval, maxval float64) float64 {
	halfN := int((n - 1) / 2)
	symmI := i - halfN // make i symmetric around 0
	if symmI == 0 {
		return minval * (-1.0 + 2.0*shift)
	}
	absI := symmI - 1
	if symmI < 0 {
		absI = -symmI
		shift = -shift
	}
	x := expVal(halfN, absI, shift, minval, maxval)
	if symmI < 0 {
		x = -x
	}
	return x
}

// X computes the representative value of x for the i'th bucket, optionally
// adjusted by the relative shift amount (shift=1.0 is the next bucket
// boundary).
func (b *Buckets) X(i int, shift float64) float64 {
	fn := linearVal
	switch b.Spacing {
	case ExponentialSpacing:
		fn = expVal
	case SymmetricExponentialSpacing:
		fn = symmExpVal
	}
	return fn(b.N, i, shift, b.Min, b.Max)
}

// Xs returns the list of representative values for all buckets, optionally
// adjusted by the relative shift amount. It always returns a newly allocated
// slice, so it is safe to modify it.
func (b *Buckets) Xs(shift float64) []float64 {
	res := make([]float64, b.N)
	for i := range res {
		res[i] = b.X(i, shift)
	}
	return res
}

// setBounds caches the n+1 bucket bounds, including the Max.
func (b *Buckets) setBounds() {
	b.Bounds = make([]float64, b.N+1)
	for i := range b.Bounds {
		b.Bounds[i] = b.X(i, 0.0)
	}
}

// Bucket computes the bucket index for a sample.
func (b *Buckets) Bucket(x float64) int {
	l := 0
	u := b.N - 1
	if x < b.Bounds[l] {
		return 0
	}
	if x >= b.Bounds[u] {
		return u
	}
	for i := 0; i < b.N && l+1 < u; i++ {
		m := int((l + u) / 2)
		if x < b.Bounds[m] {
			u = m
		} else {
			l = m
		}
	}
	if l+1 < u {
		panic(errors.Reason("l=%d + 1 < u=%d", l, u))
	}
	return l
}

// Size of the i'th bucket.
func (b *Buckets) Size(i int) float64 {
	if i < 0 || i >= b.N {
		return 0.0
	}
	return b.Bounds[i+1] - b.Bounds[i]
}

// FitTo data the bucket parameters such as spacing, min & max.  Assumes that
// data is sorted in ascending order. In case of an error, the original value is
// preserved.
func (b *Buckets) FitTo(data []float64) error {
	switch b.Spacing {
	case LinearSpacing:
		copy, err := NewBuckets(b.N, data[0], data[len(data)-1], LinearSpacing)
		if err != nil {
			return errors.Annotate(err, "failed to create buckets")
		}
		*b = *copy
		return nil
	case ExponentialSpacing:
		if data[0] < 0 || data[len(data)-1] <= 0.0 {
			copy := *b
			copy.Spacing = LinearSpacing
			if err := copy.FitTo(data); err != nil {
				return errors.Annotate(err, "failed to fit recursively")
			}
			*b = copy
			return nil
		}
		min := data[0]
		for i := 1; i < len(data) && min == 0.0; i++ {
			min = data[i]
		}
		copy, err := NewBuckets(b.N, min, data[len(data)-1], ExponentialSpacing)
		if err != nil {
			return errors.Annotate(err, "failed to create buckets")
		}
		*b = *copy
		return nil
	case SymmetricExponentialSpacing:
		if data[0] >= 0.0 {
			copy := *b
			copy.Spacing = ExponentialSpacing
			if err := copy.FitTo(data); err != nil {
				return errors.Annotate(err, "failed to fit recursively")
			}
			*b = copy
			return nil
		}
		max := math.Abs(data[len(data)-1])
		if x := math.Abs(data[0]); x > max {
			max = x
		}
		min := max
		for _, x := range data {
			if abs := math.Abs(x); abs < min && abs > 0 {
				min = abs
			}
		}
		copy, err := NewBuckets(b.N, min, max, SymmetricExponentialSpacing)
		if err != nil {
			return errors.Annotate(err, "failed to create buckets")
		}
		*b = *copy
		return nil
	}
	return errors.Reason("unsupported spacing: %s", b.Spacing)
}

// Histogram stores sample counts for each bucket.  The counts are continuous
// (float64) so that Histogram can be used to represent c.d.f.-based
// distributions derived numerically.
type Histogram struct {
	buckets *Buckets
	// All slices are expected to be of length buckets.N.
	counts   []uint    // actual sample counts, for estimating accuracy
	weights  []float64 // bucket values
	sums     []float64 // sum of samples for each bucket
	size     float64   // total sum of counts
	sumTotal float64   // total sum of samples
}

// NewHistogram creates and initializes a Histogram. It panics if buckets is
// nil.
func NewHistogram(buckets *Buckets) *Histogram {
	if buckets == nil {
		panic(errors.Reason("buckets cannot be nil"))
	}
	return &Histogram{
		buckets: buckets,
		counts:  make([]uint, buckets.N),
		weights: make([]float64, buckets.N),
		sums:    make([]float64, buckets.N),
	}
}

// Buckets value of the Histogram.
func (h *Histogram) Buckets() *Buckets { return h.buckets }

// Counts of the actual (possibly biased) samples in the Histogram. For
// p.d.f. estimates use Weights.
func (h *Histogram) Counts() []uint { return h.counts }

// Count of the i'th bucket. Returns 0 if i is out of range.
func (h *Histogram) Count(i int) uint {
	if i < 0 || i >= len(h.counts) {
		return 0.0
	}
	return h.counts[i]
}

// Weights of the buckets in the Histogram. These are the true "sizes" of the
// buckets in a traditional sense of a histogram.
func (h *Histogram) Weights() []float64 { return h.weights }

// Weight of the i'th bucket. Returns 0 if i is out of range.
func (h *Histogram) Weight(i int) float64 {
	if i < 0 || i >= len(h.weights) {
		return 0.0
	}
	return h.weights[i]
}

// Sums of samples per bucket.
func (h *Histogram) Sums() []float64 { return h.sums }

// Sum of samples for the i'th bucket. Returns 0 if i is out of range.
func (h *Histogram) Sum(i int) float64 {
	if i < 0 || i >= len(h.sums) {
		return 0.0
	}
	return h.sums[i]
}

// Size is the sum total of all counts.
func (h *Histogram) Size() float64 { return h.size }

// SumTotal of all samples.
func (h *Histogram) SumTotal() float64 { return h.sumTotal }

// Add samples to the Histogram.
func (h *Histogram) Add(xs ...float64) {
	for _, x := range xs {
		i := h.buckets.Bucket(x)
		h.counts[i]++
		h.weights[i]++
		h.sums[i] += x
		h.sumTotal += x
	}
	h.size += float64(len(xs))
}

func (h *Histogram) AddWithWeight(x, weight float64) {
	i := h.buckets.Bucket(x)
	h.counts[i]++
	h.weights[i] += weight
	xw := x * weight
	h.sums[i] += xw
	h.sumTotal += xw
	h.size += float64(weight)
}

// AddWeights to the histogram directly. Assumes len(weights) = h.Buckets().N.
func (h *Histogram) AddWeights(weights []float64) error {
	if len(weights) != len(h.weights) {
		return errors.Reason(
			"len(weights)=%d != buckets.N=%d", len(weights), len(h.weights))
	}
	for i := range weights {
		h.weights[i] += weights[i]
		h.counts[i]++
		h.size += weights[i]
		sum := h.buckets.X(i, 0.5) * weights[i]
		h.sums[i] += sum
		h.sumTotal += sum
	}
	return nil
}

// AddHistogram adds h2 samples into the Histogram. h2 must have the same
// buckets as self.
func (h *Histogram) AddHistogram(h2 *Histogram) error {
	if !h.buckets.SameAs(h2.buckets) {
		return errors.Reason("h.buckets is not the same as h2.buckets: %s != %s",
			h.buckets, h2.buckets)
	}
	for i := range h2.counts {
		h.counts[i] += h2.counts[i]
		h.weights[i] += h2.weights[i]
		h.sums[i] += h2.sums[i]
	}
	h.size += h2.size
	h.sumTotal += h2.sumTotal
	return nil
}

// X returns the mean x value of the i'th bucket, or the logical middle of the
// bucket if it has no samples.
func (h *Histogram) X(i int) float64 {
	if h.weights[i] == 0 {
		return h.buckets.X(i, 0.5)
	}
	return h.sums[i] / h.weights[i]
}

// Xs returns the list of mean values for all buckets. The slice is always newly
// allocated.
func (h *Histogram) Xs() []float64 {
	res := make([]float64, h.buckets.N)
	for i := range res {
		res[i] = h.X(i)
	}
	return res
}

// Mean computes the approximate mean of the distribution.
func (h *Histogram) Mean() float64 {
	if h.size == 0 {
		return 0.0
	}
	return h.sumTotal / h.size
}

// MAD esmimates mean absolute deviation.
func (h *Histogram) MAD() float64 {
	if h.size == 0 {
		return 0.0
	}
	mean := h.Mean()
	sum := 0.0
	for i := 0; i < h.buckets.N; i++ {
		x := h.X(i)
		dev := x - mean
		if dev < 0.0 {
			dev = -dev
		}
		sum += dev * h.weights[i]
	}
	return sum / h.size
}

// Variance esmimation.
func (h *Histogram) Variance() float64 {
	if h.size == 0 {
		return 0.0
	}
	mean := h.Mean()
	sum := 0.0
	for i := 0; i < h.buckets.N; i++ {
		x := h.X(i)
		dev := x - mean
		sum += dev * dev * h.weights[i]
	}
	return sum / h.size
}

// Sigma is the estimated standard deviation.
func (h *Histogram) Sigma() float64 {
	return math.Sqrt(h.Variance())
}

// Quantile computes the approximation of the q'th quantile, where e.g. q=0.5 is
// the 50th percentile. Panics if q is not within [0..1].
func (h *Histogram) Quantile(q float64) float64 {
	if q < 0.0 || 1.0 < q {
		panic(errors.Reason("q=%f not in [0..1]", q))
	}
	if h.size == 0.0 {
		return 0.0
	}
	var acc float64 = 0
	idx := 0
	qWeight := q * h.size
	for i, c := range h.weights {
		acc += c
		idx = i
		if acc >= qWeight {
			break
		}
	}
	accPrev := acc - h.weights[idx]
	if acc == accPrev {
		return h.buckets.Bounds[idx]
	}
	shift := 1.0 - (acc-qWeight)/(acc-accPrev)
	return h.buckets.X(idx, shift)
}

// CDF value at x, approximated using histogram weights. It is effectively an
// inverse of Quantile(), interpolating values of x when it falls between bucket
// boundaries.
func (h *Histogram) CDF(x float64) float64 {
	if x >= h.buckets.Max {
		return 1.0
	}
	if h.buckets.Spacing == SymmetricExponentialSpacing {
		if x <= -h.buckets.Max {
			return 0.0
		}
	} else if x <= h.buckets.Min {
		return 0.0
	}
	b := h.buckets.Bucket(x)
	var weightLow float64
	for i := 0; i < b; i++ {
		weightLow += h.Weight(i)
	}
	coeff := (x - h.buckets.X(b, 0.0)) / h.buckets.Size(b)
	return (weightLow + coeff*h.Weight(b)) / h.Size()
}

// Prob is the p.d.f. value at x, approximated using histogram weights.
func (h *Histogram) Prob(x float64) float64 {
	if x >= h.buckets.Max {
		return 0.0
	}
	if h.buckets.Spacing == SymmetricExponentialSpacing {
		if x <= -h.buckets.Max {
			return 0.0
		}
	} else if x <= h.buckets.Min {
		return 0.0
	}
	b := h.buckets.Bucket(x)
	shift := (x - h.buckets.X(b, 0.5)) / h.buckets.Size(b)
	var min, max float64 // p.d.f. values around x
	if shift >= 0 {
		min = h.PDF(b)
		max = h.PDF(b + 1)
	} else {
		min = h.PDF(b - 1)
		max = h.PDF(b)
		shift = 1.0 + shift
	}
	return min + shift*(max-min)
}

// PDF value at the i'th bucket. Return 0.0 if i is out of range. It integrates
// to 1.0 when dx = h.Buckets().Size(i).
func (h *Histogram) PDF(i int) float64 {
	if i < 0 || i >= len(h.weights) {
		return 0.0
	}
	if h.size == 0 {
		return 0.0
	}
	return h.weights[i] / h.size / h.buckets.Size(i)
}

// PDFs lists all the values of PDF for all the buckets. This is suitable
// for plotting against Xs().
func (h *Histogram) PDFs() []float64 {
	res := make([]float64, len(h.weights))
	for i := range h.weights {
		res[i] = h.PDF(i)
	}
	return res
}
