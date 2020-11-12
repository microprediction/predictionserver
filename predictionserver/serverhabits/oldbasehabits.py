import numpy as np
from redis.client import list_or_args
from typing import List, Union, Any, Optional

from predictionserver.futureconventions.serverconventions import ServerConventions
from predictionserver.futureconventions.typeconventions import KeyList, NameList, Value, ValueList, DelayList
from collections import OrderedDict
from itertools import zip_longest
import time
import sys
import math
from predictionserver.futureconventions.samplers import exponential_bootstrap

# Baseline private and public conventions, including some configuration and hardwired naming serverhabits, and also some
# conventions destined for microconventions package

BASE_SERVER_CONFIG_ARGS = ('history_len', 'delays', 'lagged_len', 'windows', 'obscurity')
DEFAULT_CONVENTIONS = {'min_len': 12, 'min_balance': -1, 'delays': [70, 310, 910, 3555], 'num_predictions': 225}
DEFAULT_TESTING_CONVENTIONS = {'min_len': 12, 'min_balance': -1, 'delays': [1, 5], 'num_predictions': 225}


class BaseHabits(ServerConventions):

    def __init__(self, history_len=None, lagged_len=None, max_ttl=None, windows=None, obscurity=None,
                 min_len=None, min_balance=None, num_predictions=None, delays=None):
        super().__init__()

        super().__init__(min_len=min_len, min_balance=min_balance, num_predictions=num_predictions, delays=delays)

        if windows is None:
            windows = [1e-4, 1e-3, 1e-2]
        self._WINDOWS = windows  # Sizes of neighbourhoods around truth used in countback ... don't make too big or it hits performance

        self.HISTORY_LEN = int(history_len or 1000)
        self.LAGGED_LEN = int(lagged_len or 10000)
        self.ZDELAYS = [self.DELAYS[0], self.DELAYS[-1]] # Which delays to create z-streams for
        self.CONFIRMS_MAX = 5# Maximum number of confirmations when using mset()
        self.NOISE = 0.1 / self.num_predictions  # Tie-breaking / smoothing noise added to predictions
        self.NUM_WINNERS = 10  # Maximum number of winning tickets
        self.SHRINKAGE = 0.1  # How much to shrink leaderboards

        # Implementation details: private reserved redis keys and prefixes.
        self._obscurity = (obscurity or "obscure") + self.SEP

        # Other implementation config
        self._DEFAULT_MODEL_STD = 1.0  # Noise added for self-prediction
        self._MAX_TTL = int(max_ttl or 96 * 60 * 60)  # Maximum TTL, useful for testing
        self._CREATE_COST = 500  # How much one is charged when creating a new stream

    @staticmethod
    def _descending_values(d):
        """ Present redis hashes as dict in decending order of value (volumes, leaderboards etc) """
        d_tuples = list([(ky, float(val)) for ky, val in d.items()])
        d_tuples.sort(key=lambda t: t[1], reverse=True)
        return OrderedDict(d_tuples)

    @staticmethod
    def assert_not_in_reserved_namespace(names, *args):
        names = list_or_args(names, args)
        if any(self.SEP in name for name in names):
            raise Exception("Operation attempted with a name that uses " + MicroConventions.sep())

    @staticmethod
    def to_float(values):
        # Canonical way to convert str or [str] or [[str]] to float equivalent with nan replacing None
        return np.array(values, dtype=float).tolist()

    @staticmethod
    def coerce_inputs(names: Optional[NameList] = None,
                      values: Optional[ValueList] = None,
                      write_keys: Optional[KeyList] = None,
                      name: Optional[str] = None,
                      value: Optional[Any] = None,
                      write_key: Optional[str] = None,
                      budget: Optional[int] = None,
                      budgets: Optional[List[int]] = None):
        # Convention for broadcasting optional singleton inputs to arrays
        names = names or [name]
        values = values or [value for _ in names]
        budgets = budgets or [budget for _ in names]
        write_keys = write_keys or [write_key for _ in names]
        return names, values, write_keys, budgets

    def _root_name(self, name):
        return name.split(self.SEP)[-1]

    # TODO: Move these ----------
    @staticmethod
    def delay_as_int(delay):
        """ By convention, None means no delay """
        return 0 if delay is None else int(delay)

    def percentile_name(self, name, delay):
        return self.zcurve_name(names=[name], delay=delay)

    def identity(self, name):
        return name

    @staticmethod
    def chunker(results, n):
        """ Assumes there are n*k operations and just chunks the results into groups of length k """

        def grouper(iterable, n, fillvalue=None):
            args = [iter(iterable)] * n
            return zip_longest(*args, fillvalue=fillvalue)

        m = int(len(results) / n)
        return list(grouper(iterable=results, n=m, fillvalue=None))

    # --------------------------------------------------------------------------
    #            Redis version/capability inference
    # --------------------------------------------------------------------------

    def _streams_support(self):
        # Returns True if redis streams are supported by the redis client
        # (Note that streams are not supported on fakeredis)
        try:
            record_of_test = {"time": str(time.time())}
            self.client.xadd(name='e5312d16-dc87-46d7-a2e5-f6a6225e63a5', fields=record_of_test)
            return True
        except:
            return False

    # --------------------------------------------------------------------------
    #           Default scenario generation
    # --------------------------------------------------------------------------

    def empirical_predictions(self, lagged_values):
        predictions = exponential_bootstrap(lagged=lagged_values, decay=0.005, num=self.num_predictions)
        return sorted(predictions)

    # --------------------------------------------------------------------------
    #           Time to live economics
    # --------------------------------------------------------------------------

    def _promise_ttl(self):
        return max(self.DELAYS) + self._DELAY_GRACE

    def _cost_based_history_len(self, value):
        return self.HISTORY_LEN  # TODO: Could be refined

    def _cost_based_lagged_len(self, value):
        t = time.time()
        sz = (sys.getsizeof(value) + sys.getsizeof(t)) + 10
        return int(math.ceil(10 * self.LAGGED_LEN / sz))

    def _cost_based_ttl(self, value, budget):
        """ Time to live for name implies a minimal update frequency """
        return BaseHabits._value_ttl(value=value, budget=budget, num_delays=len(self.DELAYS),
                                     max_ttl=self._MAX_TTL)

    def _cost_based_distribution_ttl(self, budget):
        """ Time to live for samples ... mostly budget independent """
        return int(max(self.DELAYS) + self._DELAY_GRACE + 60 + budget)

    @staticmethod
    def _value_ttl(value, budget, num_delays, max_ttl):
        # Assign a time to live that won't break the bank
        REPLICATION = 1 + 2 * num_delays
        BLOAT = 3
        DOLLAR = 10000.  # Credits per dollar
        COST_PER_MONTH_10MB = 1. * DOLLAR
        COST_PER_MONTH_1b = COST_PER_MONTH_10MB / (10 * 1000 * 1000)
        SECONDS_PER_DAY = 60. * 60. * 24.
        SECONDS_PER_MONTH = SECONDS_PER_DAY * 30.
        FIXED_COST_bytes = 10  # Overhead
        num_bytes = sys.getsizeof(value)
        credits_per_month = REPLICATION * BLOAT * (num_bytes + FIXED_COST_bytes) * COST_PER_MONTH_1b
        ttl_seconds = int(math.ceil(SECONDS_PER_MONTH / credits_per_month))
        ttl_seconds = budget * ttl_seconds
        ttl_seconds = min(ttl_seconds, max_ttl)
        return int(ttl_seconds)


class DefaultHabits(BaseHabits):

    def __init__(self, **kwargs):
        kwargs.update(DEFAULT_CONVENTIONS)
        super().__init__(**kwargs)


class TestingHabits(BaseHabits):

    def __init__(self, **kwargs):
        kwargs.update(DEFAULT_TESTING_CONVENTIONS)
        super().__init__(**kwargs)


if __name__ == '__main__':
    habits = TestingHabits()
    assert habits.CANCEL_SEP == '::cancel::'
    assert habits.base_url == 'http://api.microprediction.org'
