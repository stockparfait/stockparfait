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
)

// Sample stores unordered set of numerical data (float64) and computes various
// statistics over it.
type Sample struct {
	data     []float64 // keep it private, so we correctly update caches.
	sum      *float64  // cached sum of samples (for mean computation)
	sumDev   *float64  // cached sum of absolute deviations (for MAD)
	sumSqDev *float64  // cached sum of squared deviations (for variance)
}

// NewSample creates a new empty sample.
func NewSample() *Sample {
	return &Sample{}
}

// Data returns the sample data.
func (s *Sample) Data() []float64 { return s.data }

// Init sets the data in the sample to the provided slice. Note, that it reuses
// the same slice without copying. Use Copy() if you need to decouple your input
// from the Sample. It returns self for inlined declarations.
func (s *Sample) Init(data []float64) *Sample {
	s.data = data
	s.sum = nil
	s.sumDev = nil
	s.sumSqDev = nil
	return s
}

// Copy the data into Sample. The input can then be safely modified without
// affecting the Sample. It returns self for inline declarations.
func (s *Sample) Copy(data []float64) *Sample {
	cp := make([]float64, len(data))
	copy(cp, data)
	return s.Init(cp)
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
