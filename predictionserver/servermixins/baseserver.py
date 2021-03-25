from predictionserver.habits import Habits
import redis
import fakeredis
import time
import sys
import math
from collections import OrderedDict
from predictionserver.futureconventions.samplers import exponential_bootstrap
from itertools import zip_longest
from predictionserver.futureconventions.typeconventions import (
    NameList, Optional, ValueList, KeyList
)
from predictionserver.futureconventions.sepconventions import SepConventions
from typing import Any, List
from redis.client import list_or_args
import numpy as np


class BaseServer(Habits):
    _PY_REDIS_ARGS = (
        'host',
        'port',
        'db',
        'username',
        'password',
        'socket_timeout',
        'socket_keepalive',
        'socket_keepalive_options',
        'connection_pool',
        'unix_socket_path',
        'encoding',
        'encoding_errors',
        'charset',
        'errors',
        'decode_responses',
        'retry_on_timeout',
        'ssl',
        'ssl_keyfile',
        'ssl_certfile',
        'ssl_cert_reqs',
        'ssl_ca_certs',
        'ssl_check_hostname',
        'max_connections',
        'single_connection_client',
        'health_check_interval',
        'client_name'
    )
    _FAKE_REDIS_ARGS = ('decode_responses',)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # Creates fake redis instance on initialization.
        # Use connect() to modify
        self.client = self._make_redis_client(decode_responses=True)
        # Other implementation config
        self._DEFAULT_MODEL_STD = 1.0  # Noise added for self-prediction
        self._MAX_TTL = 96 * 60 * 60  # Maximum TTL, useful for testing
        self._CREATE_COST = 500  # How much one is charged when creating a new stream
        # Sizes of neighbourhoods around truth used in countback ... don't make
        # too big or it hits performance
        self._WINDOWS = [1e-4, 1e-3, 1e-2]
        self.HISTORY_LEN = 1000
        self.LAGGED_LEN = 10000
        self.CONFIRMS_MAX = 5  # Maximum number of confirmations when using mset()

    def connect(self, **kwargs):
        """ Establish client to real or fake redis database """
        # Also activate self.obscurity() by supplying a password
        self.client = self._make_redis_client(**kwargs)
        obscurity = kwargs.get('obscurity')
        if obscurity:
            self.set_obscurity(secret=obscurity)

    def _make_redis_client(self, **kwargs):
        kwargs["decode_responses"] = True  # Very important in this implementation
        is_real = "host" in kwargs
        KWARGS = self._PY_REDIS_ARGS if is_real else self._FAKE_REDIS_ARGS
        redis_kwargs = dict()
        for k in KWARGS:
            if k in kwargs:
                redis_kwargs[k] = kwargs[k]
        if is_real:
            return redis.StrictRedis(**redis_kwargs)
        else:
            return fakeredis.FakeStrictRedis(**redis_kwargs)

    def execute_one(self, method, **kwargs):
        pipe = self.client.pipeline()
        pipe = method(pipe=pipe, **kwargs)
        execution = pipe.execute()
        return execution[0]

    def execute_many(
            self,
            method,
            varying_kwargs: [dict],
            methods=None,
            transaction=False,
            shard_hint=None,
            **kwargs
    ):
        """
        :param method:              Double underscored method modifying a redis pipeline
        :param varying_kwargs:      List of arguments to send to the method
        :param kwargs:              List of arguments broadcast to every method call
        :param methods:             Optional list of methods. Otherwise assumes the same
                                    method is called each time
        :return: List of execution results
        """
        methods = methods or [method for _ in varying_kwargs]
        assert len(methods) == len(varying_kwargs)
        pipe = self.client.pipeline(transaction=transaction, shard_hint=shard_hint)
        for mthd, vry in zip(methods, varying_kwargs):
            pipe = mthd(pipe=pipe, **vry, **kwargs)
        execution = pipe.execute()
        return execution

    @staticmethod
    def coerce_inputs(
            names: Optional[NameList] = None,
            values: Optional[ValueList] = None,
            write_keys: Optional[KeyList] = None,
            name: Optional[str] = None,
            value: Optional[Any] = None,
            write_key: Optional[str] = None,
            budget: Optional[int] = None,
            budgets: Optional[List[int]] = None
    ):
        # Convention for broadcasting optional singleton inputs to arrays
        names = names or [name]
        values = values or [value for _ in names]
        budgets = budgets or [budget for _ in names]
        write_keys = write_keys or [write_key for _ in names]
        return names, values, write_keys, budgets

    @staticmethod
    def _descending_values(d):
        """
        Present redis hashes as dict in decending order of value (volumes, leaderboards)
        """
        d_tuples = list([(ky, float(val)) for ky, val in d.items()])
        d_tuples.sort(key=lambda t: t[1], reverse=True)
        return OrderedDict(d_tuples)

    @staticmethod
    def assert_not_in_reserved_namespace(names, *args):
        names = list_or_args(names, args)
        if any(SepConventions.sep() in name for name in names):
            raise Exception(
                "Operation attempted with a name that uses " +
                SepConventions.sep())

    @staticmethod
    def to_float(values):
        # Canonical way to convert str or [str] or [[str]] to float equivalent
        # with nan replacing None
        return np.array(values, dtype=float).tolist()

    def _root_name(self, name):
        return name.split(SepConventions.sep())[-1]

    # TODO: Move these ----------
    @staticmethod
    def delay_as_int(delay):
        """ By convention, None means no delay """
        return 0 if delay is None else int(delay)

    def identity(self, name):
        return name

    @staticmethod
    def chunker(results, n):
        """
        Assume there are n*k operations and just chunks the results into groups of length k
        """

        def grouper(iterable, n, fillvalue=None):
            args = [iter(iterable)] * n
            return zip_longest(*args, fillvalue=fillvalue)

        m = int(len(results) / n)
        return list(grouper(iterable=results, n=m, fillvalue=None))

    # --------------------------------------------------------------------------
    #           Time to live economics
    # --------------------------------------------------------------------------

    def _promise_ttl(self):
        return 10 + self._DELAY_GRACE

    def _cost_based_history_len(self, value):
        return self.HISTORY_LEN  # TODO: Could be refined

    def _cost_based_lagged_len(self, value):
        t = time.time()
        sz = (sys.getsizeof(value) + sys.getsizeof(t)) + 10
        return int(math.ceil(10 * self.LAGGED_LEN / sz))

    def _cost_based_ttl(self, budget, value):
        """ Time to live for name implies a minimal update frequency """
        return self._value_ttl(
            value=value,
            budget=budget,
            num_delays=5,
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
        credits_per_month = REPLICATION * BLOAT * \
            (num_bytes + FIXED_COST_bytes) * COST_PER_MONTH_1b
        ttl_seconds = int(math.ceil(SECONDS_PER_MONTH / credits_per_month))
        ttl_seconds = budget * ttl_seconds
        ttl_seconds = min(ttl_seconds, max_ttl)
        return int(ttl_seconds)

    # --------------------------------------------------------------------------
    #            Redis version/capability inference
    # --------------------------------------------------------------------------

    def _streams_support(self):
        # Returns True if redis streams are supported by the redis client
        # (Note that streams are not supported on fakeredis)
        try:
            record_of_test = {"time": str(time.time())}
            self.client.xadd(
                name='e5312d16-dc87-46d7-a2e5-f6a6225e63a5',
                fields=record_of_test)
            return True
        except BaseException:
            return False

    # --------------------------------------------------------------------------
    #           Default scenario generation
    # --------------------------------------------------------------------------
    # Used by the sponsor of a stream

    def empirical_predictions(self, lagged_values):
        predictions = exponential_bootstrap(
            lagged=lagged_values, decay=0.005, num=self.NUM_PREDICTIONS)
        return sorted(predictions)
