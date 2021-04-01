import random
import bisect
import numpy as np


# --------------------------------------------------------------------------
#          Recency bootstrappy things
# --------------------------------------------------------------------------
# Easy to understand benchmarks. Feel free to contribute more.


def exponential_bootstrap(lagged, decay, num, as_process=None):
    as_process = as_process or is_process(lagged)
    return differenced_bootstrap(
        lagged=lagged,
        decay=decay,
        num=num) if as_process else independent_bootstrap(
        lagged=lagged,
        decay=decay,
        num=num,
    )


def independent_bootstrap(lagged, decay, num):
    """ One parameter jiggled bootstrap favouring more recent observations
          lagged  [ float ]     List most recent observation first
          decay    float        Coefficient in exp(-a k) that weights samples
          num      int          Number of scenarios requested
           :returns  [ float ]  Statistical sample
    """
    weights = list(np.exp([-decay * k for k in range(len(lagged))]))
    empirical_sample = weighted_random_sample(
        population=lagged, weights=weights, num=num
    )
    noise = np.random.randn(num)
    return [x + decay * eps for x, eps in zip(empirical_sample, noise)]


def differenced_bootstrap(lagged, decay, num):
    """
    One parameter jiggled bootstrap favouring more recent observations
    (applied to differences processes)
    """
    safe_diff_lagged = np.diff(list(lagged) + [0., 0.])
    diff_samples = independent_bootstrap(
        lagged=safe_diff_lagged, decay=decay, num=num
    )
    return [lagged[0] + dx for dx in diff_samples]


# --------------------------------------------------------------------------
#            Diagnostics
# --------------------------------------------------------------------------

def sign_changes(lagged):
    return np.nansum([
        abs(d) > 1.5 for d in np.diff(np.sign(list(lagged) + [0., 0.]))
    ])


def is_process(lagged):
    return sign_changes(np.diff(lagged)) > 2 * sign_changes(lagged)


# --------------------------------------------------------------------------
#            Gaussian
# --------------------------------------------------------------------------

def gaussian_samples(lagged, num, as_process=None):
    as_process = as_process or is_process(lagged)
    return diff_gaussian_samples(
        lagged=lagged,
        num=num) if as_process else independent_gaussian_samples(
        lagged=lagged,
        num=num)


def evenly_spaced_percentles(num):
    return [1. / (2 * num)] + \
        list(1. / (2 * num) + np.cumsum((1 / num) * np.ones(num - 1)))


def independent_gaussian_samples(lagged, num):
    shrunk_std = np.nanstd(list(lagged) + [0.01, -0.01])
    shrunk_mean = np.nanmean(lagged + [0.0])
    norminv = _norminv_function()
    return [
        shrunk_mean + shrunk_std * norminv(p) for p in evenly_spaced_percentles(num)
    ]


def diff_gaussian_samples(lagged, num):
    """ Samples from differences """
    safe_diff_lagged = np.diff(list(lagged) + [0., 0.])
    diff_samples = independent_gaussian_samples(lagged=safe_diff_lagged, num=num)
    return [lagged[0] + dx for dx in diff_samples]


def _norminv_function():
    try:
        from statistics import NormalDist
        return NormalDist(mu=0, sigma=1.0).inv_cdf
    except ImportError:
        from scipy.stats import norm
        return norm.ppf


# --------------------------------------------------------------------------
#            Helpers
# --------------------------------------------------------------------------


def weighted_random_sample(weights, num, population=None):
    """ Weighted version of random.sample()  """
    wrg = WeightedRandomGenerator(weights)
    ndx = [wrg() for _ in range(num)]
    return ndx if population is None else [population[k] for k in ndx]


class WeightedRandomGenerator(object):
    def __init__(self, weights):
        self.totals = []
        running_total = 0

        for w in weights:
            running_total += w
            self.totals.append(running_total)

    def next(self):
        rnd = random.random() * self.totals[-1]
        return bisect.bisect_right(self.totals, rnd)

    def __call__(self):
        return self.next()
