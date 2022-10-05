# Statistics package

## Variable substitution in Monte Carlo integration

Given the original integral:

```
I = \integral_{x_min..x_max} f(x) dx
```

replace `x` by `x(t)`, so the integral becomes over `dt`:

```
I = integral_{t_min..t_max} f(x(t)) x'(t) dt
```

where `x(t_min) = x_min`, `x(t_max) = x_max`, and `x'(t) = dx/dt` is the
derivative of `x(t)` over `t`.

The interesting case supported here is an `N`-dimensional integral over a vector
`X=(x_1, ..., x_N)` in `R^N`, that is the `N`-dimensional real hyperspace. The
original integral is assumed to be of the form:

```
I = E[g(X)] = \integral g(X)*f(X)*dX
```

where `f(X)` is the p.d.f. of some multivariate distribution of `X`.  The
simplest way to compute it is to generate random samples of `X` using the same
distribution.  Then the integral `I` can be approximated as:

```
I ~= 1/N * sum_{i=1..K} g(X_i)
```

for `K` number of samples.

In practice, the distribution `f(X)` may require too many samples to generate
enough samples in the area of interest, e.g. where `g(X)` is sufficiently large
and significantly contributes to the integral. Therefore, it may be beneficial
to replace each `x` in the vector `X` with another variable `t` uniformly
distributed in `(-1..1)`, such that `x(t -> -1) -> -Inf`, `x(t -> 1) -> Inf`,
and `x(t)` is monotonically increasing and differentiable over the entire `R`.

Specificially, our `g(X)` will often be a unit function on a subspace, for
computing a bucket value in a histogram:

g(X) = (sum(X) in [low .. high]) ? 1 : 0

The substitution is
```
x(t) = r * t / (1 - t^(2*b))
```

where `r` controls the width of a near-uniform distribution of `x` values around
zero, and `b` controls the portion of samples falling beyond the interval
`[-r..r]`.

However, rather than computing each bucket value separately, we will be sampling
`x` over the entire range using this method, and incrementing the appropriate
bucket by `f(x(t))*x'(t)`, thus computing many `g(x)`'s in one go.

