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

	"github.com/stockparfait/errors"
)

// Sample stores unordered set of numerical data (float64) and computes various
// statistics over it.
type Sample struct {
	data     []float64 // keep it private, so we correctly update caches.
	sum      *float64  // cached sum of samples (for mean computation)
	sumDev   *float64  // cached sum of absolute deviations (for MAD)
	sumSqDev *float64  // cached sum of squared deviations (for variance)
}

// NewSample creates a new sample initialized with data. Note, that it reuses
// the slice without copying. Use Copy() if you need to decouple your input from
// the Sample.
func NewSample(data []float64) *Sample {
	return &Sample{data: data}
}

// Data returns the sample data.
func (s *Sample) Data() []float64 { return s.data }

// Copy creates a deep copy of the Sample. This can be useful, e.g. like this:
//
//	s := NewSample(data).Copy()
//	// can safely modify data in place without affecting s.
func (s *Sample) Copy() *Sample {
	data := make([]float64, len(s.data))
	copy(data, s.data)
	return &Sample{
		data:     data,
		sum:      s.sum,
		sumDev:   s.sumDev,
		sumSqDev: s.sumSqDev,
	}
}

// Sum of samples, cached.
func (s *Sample) Sum() float64 {
	if s.sum == nil {
		sum := 0.0
		for _, d := range s.data {
			sum += d
		}
		s.sum = &sum
	}
	return *s.sum
}

// Mean computes the mean of the Sample, cached.
func (s *Sample) Mean() float64 {
	if len(s.data) == 0 {
		return 0.0
	}
	return s.Sum() / float64(len(s.data))
}

// SumDev computes the sum of absolute deviations from the mean, cached.
func (s *Sample) SumDev() float64 {
	if s.sumDev == nil {
		mean := s.Mean()
		sumDev := 0.0
		for _, d := range s.data {
			sumDev += math.Abs(d - mean)
		}
		s.sumDev = &sumDev
	}
	return *s.sumDev
}

// MAD computes mean absolute deviation of the Sample, cached.
func (s *Sample) MAD() float64 {
	if len(s.data) == 0 {
		return 0.0
	}
	return s.SumDev() / float64(len(s.data))
}

// SumSquaredDev computes the sum of squared deviations from the mean, cached.
func (s *Sample) SumSquaredDev() float64 {
	if s.sumSqDev == nil {
		mean := s.Mean()
		v := 0.0
		for _, d := range s.data {
			v += (d - mean) * (d - mean)
		}
		s.sumSqDev = &v
	}
	return *s.sumSqDev
}

// Variance of the Sample (sigma squared), cached.
func (s *Sample) Variance() float64 {
	if len(s.data) == 0 {
		return 0.0
	}
	return s.SumSquaredDev() / float64(len(s.data))
}

// Sigma computes the standard deviation of the Sample, cached.
func (s *Sample) Sigma() float64 {
	return math.Sqrt(s.Variance())
}

// Normalize creates a new Sample of {(x - mean) / MAD}, thus its Mean and MAD
// are 0 and 1, respectively.
func (s *Sample) Normalize() (*Sample, error) {
	mad := s.MAD()
	if mad == 0.0 || math.IsInf(mad, 0) {
		return nil, errors.Reason("MAD=%g must be non-zero and finite", mad)
	}
	mean := s.Mean()
	data := make([]float64, len(s.data))
	for i, d := range s.data {
		data[i] = (d - mean) / mad
	}
	return NewSample(data), nil
}
