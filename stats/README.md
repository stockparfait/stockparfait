# Statistics package

## Variable substitution in Monte Carlo integration

Given the original integral:

$$
I = \int_{x_{min}}^{x_{max}} f(x) dx
$$

replace $x$ by $x(t)$, so the integral becomes over $dt$:

$$
I = \int_{t_{min}}^{t_{max}} f(x(t)) x'(t) dt
$$

where $x(t_{min}) = x_{min}$, $x(t_{max}) = x_{max}$, and $x'(t) = \frac{dx}{dt}$ is the
derivative of $x(t)$ over $t$.

The interesting case supported here is an $N$-dimensional integral over a vector
$X=(x_1, \ldots, x_N)$ in $\mathbb{R}^N$, that is the $N$-dimensional real hyperspace. The
original integral is assumed to be of the form:

$$
I = E[g(X)] = \int_{-\infty}^\infty g(X)f(X)dX
$$

where $f(X)$ is the p.d.f. of some multivariate distribution of $X$.  The
simplest way to compute it is to generate random samples of $X$ using the same
distribution.  Then the integral $I$ can be approximated as:

$$
I \approx \frac{1}{N} \cdot \sum_{i=1}^K g(X_i)
$$

for $K$ number of samples.

In practice, the distribution $f(X)$ may require too many samples to generate
enough samples in the area of interest, e.g. where $g(X)$ is sufficiently large
and significantly contributes to the integral. Therefore, it may be beneficial
to replace each $x$ in the vector $X$ with another variable $t$ uniformly
distributed in $(-1..1)$, such that $x(t \to -1) \to -\infty$, $x(t \to 1) \to +\infty`,
$x(t)$ is monotonically increasing and differentiable over $(-1..1)$, and the
probability of "interesting" values of $x(t)$ is significant, so the number of
required samples can be reduced.

Specificially, our $g(X)$ will often be a unit function on a subspace, usually
for computing a bucket value in a histogram for the $N$-compounded sample:

$$
g(X) = \left(\sum_i X_i \in [\mathrm{low} .. \mathrm{high}]\right) ? 1 : 0
$$

The substitution is:

$$
x(t) = \frac{rt}{1 - t^{2b}}.
$$

where $r$ controls the width of a near-uniform distribution of $x$ values around
zero, and $b$ controls the portion of samples falling beyond the interval
$[-r..r]$.

Empirically, for the $N$-sum over $[\mathrm{low}..\mathrm{high}]$, a good choice of parameters is:

$$
r = \frac{\max(\mid \mathrm{low} \mid, \mid \mathrm{high} \mid)}{\sqrt{N}}
$$

$$
b = \mathrm{ceiling}(\sqrt{N})
$$

However, rather than computing each bucket value separately, since we are
effectively sampling $x$ over the entire range, we can use every sample to
increase the appropriate bucket by $f(x(t))x'(t)$, thus computing many $g(x)$'s
in one go. The value of $r$ in this case is the maximum absolute value in the
buckets' range.

