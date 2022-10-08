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

// PreciseEnough determines if the value of x with an estimated deviation is
// within epsilon neighborhood of its true value. This can be used as a
// termination criteria in iterative approximation methods when a desired
// precision has been reached.
//
// Note, that epsilon provides a relative precision: the true value of x is
// assumed to be within [x-dev..x+dev] interval, and the precision is reached
// when dev/x < epsilon for |x| >= 1, otherwise dev < epsilon.
func PreciseEnough(x, deviation, epsilon float64, relative bool) bool {
	if deviation <= 0 {
		return true
	}
	if epsilon <= 0 {
		return false
	}
	x = math.Abs(x)
	if relative {
		return deviation < epsilon*x
	}
	return deviation < epsilon
}

// StandardError accumulates and estimates the stardand deviation of an online
// sequence of samples. The accumulation of the stardand deviation is done in a
// computationally stable way using a generalization of the Youngs and Cramer
// formulas, a variant of the more popular Welford's algorithm.
//
// A zero value of StandardError is ready for use, and represents 0 samples.
type StandardError struct {
	n          uint    // number of samples
	sum        float64 // sum of samples
	sumSquares float64 // sum of (x_i - sum/n)^2
}

// Add a single sample.
func (e *StandardError) Add(x float64) {
	e.Merge(StandardError{n: 1, sum: x})
}

// AddZeros adds n zero-valued samples.
func (e *StandardError) AddZeros(n uint) {
	e.Merge(StandardError{n: n})
}

// Merge the other StardandError into e, so the resulting error estimate is for
// the union of samples.
func (e *StandardError) Merge(other StandardError) {
	if e.n == 0 {
		*e = other
		return
	}
	if other.n == 0 {
		return
	}
	n := e.n + other.n
	adj := float64(other.n)/float64(e.n)*e.sum - other.sum
	adj *= adj
	adj *= float64(e.n) / float64(other.n) / float64(n)
	*e = StandardError{
		n:          n,
		sum:        e.sum + other.sum,
		sumSquares: e.sumSquares + other.sumSquares + adj,
	}
}

// N returns the number of accumulated samples.
func (e StandardError) N() uint { return e.n }

// Mean value of all samples.
func (e StandardError) Mean() float64 {
	if e.n == 0 {
		return 0
	}
	return e.sum / float64(e.n)
}

// Variance of the accumulated samples.
func (e StandardError) Variance() float64 {
	if e.n == 0 {
		return 0
	}
	return e.sumSquares / float64(e.n)
}

// Sigma is the standard deviation of the accumulated samples.
func (e StandardError) Sigma() float64 {
	return math.Sqrt(e.Variance())
}

// ExpectationMC computes a (potentially partial) expectation integral:
// \integral_{ low .. high } [ f(x) * d.Prob(x) * dx ] using the simple
// Monte-Carlo method of sampling f(x) with the given distribution sampler and
// computing the average. The bounds are inclusive. Note, that low may be -Inf,
// and high may be +Inf.
//
// The sampling stops either when the maxIter samples have been reached, or when
// the estimated standard error becomes less than the required relative or
// absolute precision. See PreciseEnough for the exact semantics.
//
// In any case, minIter iterations are guaranteed; it should normally be a small
// number (e.g. 100) to accumulate a reasonable initial error estimate.
func ExpectationMC(f func(x float64) float64, random func() float64,
	low, high float64, minIter, maxIter uint, precision float64, relative bool) float64 {
	var count uint = 0
	var sum, result float64
	var stdErr StandardError

	for i := uint(0); i < maxIter; i++ {
		x := random()
		count++
		if x < low || high < x {
			continue
		}
		sum += f(x)
		c := float64(count)
		result = sum / c
		stdErr.Add(result)
		if stdErr.N() < count {
			stdErr.AddZeros(count - stdErr.N())
		}
		if count < minIter {
			continue // too few samples to estimate error
		}
		if PreciseEnough(result, stdErr.Sigma(), precision, relative) {
			break
		}
	}
	return result
}

// VarSubst computes the value of
//
//   x(t) = shift + scale * t / (1 - t^(2*power))
//
// to be used as a variable substitution in an integral over x in
// (-Inf..Inf). The new bounds for t become (-1..1), excluding the boundaries.
//
// In Monte Carlo integration, the integral_{-Inf..Inf} f(x)dx is approximated
// by the sample average E[ f(x(t))*x'(t) ] for a uniformly distributed t over
// (-1..1).
func VarSubst(t, scale, power, shift float64) float64 {
	t2p := math.Pow(t*t, power) // use t*t so b could be fractional
	return shift + scale*t/(1-t2p)
}

// VarPrime is the value of x'(t), the first derivative of x(t).
func VarPrime(t, scale, power float64) float64 {
	t2p := math.Pow(t*t, power)
	return scale * (1 + (2*power-1)*t2p) / ((1 - t2p) * (1 - t2p))
}
