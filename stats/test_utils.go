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

// roundFixed rounds x to the fixed number of decimal places. This is useful for
// rounding around zero, since it has no well-defined number of significant
// digits.
func roundFixed(x float64, digits int) float64 {
	f := math.Pow10(digits)
	return math.Round(x*f) / f
}

// round x to the given number of significant decimal digits, for approximate
// comparisons in tests.
func round(x float64, places int) float64 {
	order := 0
	if x != 0.0 {
		order = int(math.Log10(math.Abs(x)))
	}
	if order >= 0 {
		order++
	}
	f := math.Pow10(places - order)
	return math.Round(x*f) / f
}

// roundSlice rounds the elements of the slice to the given number of
// significant decimal digits, for approximate comparisons in tests.
func roundSlice(s []float64, places int) []float64 {
	res := make([]float64, len(s))
	for i := range s {
		res[i] = round(s[i], places)
	}
	return res
}
