from predictionserver.serverhabits.performancehabits import PerformanceHabits
from predictionserver.servermixins.baseserver import BaseServer
from predictionserver.servermixins.ownershipserver import OwnershipServer
from predictionserver.futureconventions.performanceconventions import PerformanceGranularity
from pprint import pprint
import random

# Remark: try to use strict methods as they anticipate future performance
# granularity beyond write_key only.


class PerformanceServer(PerformanceHabits, BaseServer):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def get_performance(self, write_key, name, delay):
        return self.get_performance_strict(granularity=PerformanceGranularity.write_key,
                                           write_key=write_key, name=name, delay=int(delay))

    def get_performance_strict(
            self,
            granularity: PerformanceGranularity,
            write_key: str,
            name: str,
            delay: int):
        return self._get_performance_implementation(
            granularity=granularity, write_key=write_key, name=name, delay=delay)

    def get_performances(self, write_key):
        return self.get_performances_strict(
            granularity=PerformanceGranularity.write_key,
            write_key=write_key)

    def get_performances_strict(self, granularity, write_key):
        return self._get_performances_implementation(
            granularity=granularity, write_key=write_key)

    def delete_performances(self, write_key):
        return self.delete_performances_strict(
            granularity=PerformanceGranularity.write_key,
            write_key=write_key)

    def delete_performances_strict(self, granularity: PerformanceGranularity, **kwargs):
        """ Reset all performances to zero """
        if granularity == PerformanceGranularity.write_key:
            self._delete_performances_implementation(granularity=granularity, **kwargs)

    def shrink_performances(self, multiplier: float, **kwargs):
        """ Multiply all performances by a scalar (typically between 0 and 1) """
        return self.shrink_performances_strict(
            granularity=PerformanceGranularity.write_key,
            multiplier=multiplier,
            **kwargs)

    def shrink_performances_strict(
            self,
            granularity: PerformanceGranularity,
            multiplier: float,
            **kwargs):
        """ Multiply all performances by a scalar (typically between 0 and 1) """
        return self._shrink_performances_implementation(
            granularity=granularity, multiplier=multiplier, **kwargs)

    # -------------
    #   System use
    # --------------

    def _performance_key(self, name, delay):
        return self.horizon_name(name=name, delay=delay)

    def __incr_performance(self, pipe, write_key, name, delay, amount):
        pipe.hincrbyfloat(name=self.performance_name(write_key=write_key),
                          key=self._performance_key(name=name, delay=delay), amount=amount)
        if self._PERFORMANCE_BACKWARD_COMPATIBLE:
            pipe.hincrbyfloat(name=self.performance_name_old(write_key=write_key),
                              key=self.horizon_name(name=name, delay=delay), amount=amount)
        return pipe

    # -------------
    #   Implementation
    # --------------

    def _get_performances_implementation(self, granularity, write_key):
        performance = self.client.hgetall(
            name=self.performance_name(
                granularity=granularity,
                write_key=write_key))
        return self._descending_values(performance)

    def _get_performance_implementation(self, granularity, write_key, name, delay):
        key = self._performance_key(name=name, delay=delay)
        location = self.performance_name(granularity=granularity, write_key=write_key)
        try:
            performance = self.client.hget(name=location, key=key)
        except Exception:
            performance = 0
        return performance

    def _delete_performances_implementation(
            self, granularity: PerformanceGranularity, **kwargs):
        if self._PERFORMANCE_BACKWARD_COMPATIBLE:
            self.client.delete(self.performance_name_old(granularity=granularity, **kwargs))
        return self.client.delete(self.performance_name(granularity=granularity, **kwargs))

    def _shrink_performances_implementation(
            self,
            granularity: PerformanceGranularity,
            multiplier: float,
            **kwargs):
        """ Multiply performances by a scalar """
        pn = self.performance_name(variety=granularity, **kwargs)
        temporary_key = 'temporary_' + \
            ''.join([random.choice(['a', 'b', 'c']) for _ in range(20)])
        shrink_pipe = self.client.pipeline(transaction=True)
        shrink_pipe.zunionstore(dest=temporary_key, keys={pn: multiplier})
        shrink_pipe.zunionstore(dest=pn, keys={temporary_key: 1})
        exec = shrink_pipe.execute()
        return {pn: exec}


class StandalonePerformanceServer(PerformanceServer, OwnershipServer, BaseServer):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)


if __name__ == '__main__':
    from predictionserver.collider_config_private import REDIZ_COLLIDER_CONFIG, EMBLOSSOM_MOTH
    server = StandalonePerformanceServer(**REDIZ_COLLIDER_CONFIG)
    perf = server.get_performances(write_key=EMBLOSSOM_MOTH)
    pprint(perf)
