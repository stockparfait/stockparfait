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
func PreciseEnough(x, deviation, epsilon float64) bool {
	if deviation <= 0 {
		return true
	}
	if epsilon <= 0 {
		return false
	}
	x = math.Abs(x)
	if x < 1 {
		x = 1
	}
	return deviation < epsilon*x
}

// ExpectationMC computes a (potentially partial) expectation integral:
// \integral_{ low .. high } [ f(x) * d.Prob(x) * dx ] using the simple
// Monte-Carlo method of sampling f(x) with the given distribution sampler and
// computing the average. The bounds are inclusive. Note, that low may be -Inf,
// and high may be +Inf.
//
// The sampling stops either when the maxIter samples have been reached, or when
// the average relative deviation of the result becomes less than the required
// precision.
func ExpectationMC(f func(x float64) float64, random func() float64,
	low, high float64, maxIter int, precision float64) float64 {
	var count = 0
	var sum, devSum, result float64

	for i := 0; i < maxIter; i++ {
		x := random()
		prevRes := result
		count++
		if x < low || high < x {
			continue
		}
		sum += f(x)
		if count == 1 { // no useful prevRes yet
			continue
		}
		result = sum / float64(count)
		devSum += math.Abs(prevRes - result)
		if PreciseEnough(result, devSum/float64(count), precision) {
			break
		}
	}
	return result
}

/// Methods for integral variable substitution in Monte Carlo integration.
//
// Given the original I = \integral_{x_min..x_max} f(x) dx, replace x by t: I =
// integral_{t_min..t_max} f(x(t)) x'(t) dt, where x(t_min) = x_min, x(t_max) =
// x_max, and x'(t) = dx/dt is the derivative of x(t) over t.
//
// The interesting case supported here is an N-dimensional integral over a
// vector X=(x_1, ..., x_N) in R^N, that is the entire N-dimensional real
// hyperspace. The original integral is assumed to be of the form:
//
// I = E[g(X)] = \integral g(X)*f(X)*dX
//
// where f(X) is a p.d.f. of a distribution. The simplest way to compute it is
// to generate random samples of X using the same distribution; then:
//
// I ~= 1/N * sum_{i=1..K} g(X_i) for K number of such samples.
//
// In practice, the distribution f(X) may require too many samples to generate
// enough samples in the area of interest, e.g. where g(X) is sufficiently large
// and significantly contributes to the integral. Therefore, it may be
// beneficial to replace it with another variable t uniformly distributed in
// (-1..1), such that x(-1) = -Inf, x(1) = Inf, and x(t) is monotonically
// increasing and differentiable over the entire R.
//
// Specificially, our g(X) will often be a unit function on a range, for
// computing a bucket value in a histogram:
//
// g(x) = (x in [low .. high]) ? 1 : 0
//
// The substitution is x(t) = r * t / (1 - t^(2*b)), where r controls the width
// of a near-uniform distribution of x values around zero, and b controls the
// portion of samples falling beyond the interval [-r..r].
//
// However, rather than computing each bucket value separately, we will be
// sampling x over the entire range using this method, and incrementing the
// appropriate bucket by f(x(t))*x'(t), thus computing many g(x)'s in one go.

// VarSubst computes the value of x(t) = r * t / (1 - t^(2*b)) to be used as a
// variable substitution in an integral over x in (-Inf..Inf). The new bounds
// for t become (-1..1), excluding the boundaries.
func VarSubst(t, r, b float64) float64 {
	return r * t / (1 - math.Pow(t*t, b))
}

// VarPrime is the value of x'(t), the first derivative of x(t).
func VarPrime(t, r, b float64) float64 {
	t2b := math.Pow(t, 2*b)
	return r * (1 + (2*b-1)*t2b) / ((1 - t2b) * (1 - t2b))
}
