from predictionserver.serverhabits.metrichabits import MetricHabits, MetricType, MetricGranularity
from predictionserver.servermixins.memoserver import MemoServer
from predictionserver.servermixins.ownershipserver import OwnershipServer


class MetricServer(MemoServer, OwnershipServer):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def get_metric(self, metric: MetricType, granularity: MetricGranularity=None, **kwargs)->float:
        """ Retrieve one metric """
        if granularity is None:
            granularity = self.metric_granularity_inference(**kwargs)
        return self._get_metric_implementation(metric=metric, granularity=granularity, **kwargs)

    def get_metrics(self, metric: MetricType, granularity: MetricGranularity, **kwargs):
        """ Retrieve entire table """
        # Not intended to be exposed
        return self._get_metrics_implementation(metric=metric, granularity=granularity, **kwargs)

    # ---------------- #
    #  System use      #
    # ---------------- #

    def incr_metric(self, metric, granularity, amount, **kwargs):
        return self.execute_one(method=self._pipe_incr_metric, metric=metric, granularity=granularity, amount=amount,
                                **kwargs)

    def set_metric(self, metric: MetricType, granularity: MetricGranularity, value: float, **kwargs):
        res = self.execute_one(method=self._pipe_set_metric, metric=metric, granularity=granularity, value=value,
                                **kwargs)
        return 1

    # ----------------------------- #
    #  Special cases / examples     #
    # ----------------------------- #

    def get_stream_metric(self, metric:MetricType, name:str):
        return self.get_metric(metric=metric, granularity=MetricGranularity.name, name=name)

    def get_horizon_metric(self, metric: MetricType, name: str, delay:int):
        return self.get_metric(metric=metric, granularity=MetricGranularity.name_and_delay, name=name, delay=delay)

    def get_public_owner_metric(self, metric: MetricType, code: str):
        return self.get_metric(metric=metric, granularity=MetricGranularity.code, code=code)

    def get_private_owner_metric(self, metric: MetricType, write_key: str):
        return self.get_metric(metric=metric, granularity=MetricGranularity.write_key, write_key=write_key)

    def get_stream_budget(self, name: str) -> float:
        return self.get_metric(metric=MetricType.budget, granularity=MetricGranularity.name, name=name)

    def get_horizon_budget(self, name:str, delay:int)->float:
        return self.get_horizon_metric(metric=MetricType.budget, name=name, delay=delay)

    def get_horizon_budgets(self) -> float:
        return self.get_metrics(granularity=MetricGranularity.name_and_delay, metric=MetricType.budget)

    def get_stream_volume(self, name):
        return self.get_metric(metric=MetricType.volume, granularity=MetricGranularity.name, name=name)

    def get_stream_volumes(self):
        return self.get_metrics(metric=MetricType.volume, granularity=MetricGranularity.name)

    def get_stream_budgets(self):
        return self.get_metrics(metric=MetricType.budget, granularity=MetricGranularity.name)




    # --------------- #
    # Implementation  #
    # --------------- #

    def _get_metric_implementation(self, metric, granularity, **kwargs):
        raw = self.execute_one(method=self._pipe_get_metric, metric=metric, granularity=granularity, **kwargs)
        return float(raw) if raw is not None else 0

    def _get_metrics_implementation(self, metric, granularity, **kwargs):
        raw_metrics = self.execute_one(method=self._pipe_get_metrics, metric=metric, granularity=granularity, **kwargs)
        return self._descending_values(raw_metrics)

    def _pipe_get_metric(self, pipe, metric: MetricType, granularity: MetricGranularity, **kwargs):
        location, key = self._metric_location_and_key(metric=metric, granularity=granularity, **kwargs)
        pipe.hget(name=location, key=key)
        return pipe

    def _pipe_get_metrics(self, pipe, metric: MetricType, granularity: MetricGranularity, **kwargs):
        location = self.metric_name_strict(metric=metric, granularity=granularity)
        pipe.hgetall(name=location)
        return pipe

    def _pipe_set_metric(self, pipe, metric: MetricType, granularity: MetricGranularity, value, **kwargs):
        location, key = self._metric_location_and_key(metric=metric, granularity=granularity, **kwargs)
        pipe.hset(name=location, key=key, value=value )
        return pipe

    def _pipe_incr_metric(self, pipe, metric: MetricType, granularity: MetricGranularity, amount, **kwargs):
        location, key = self._metric_location_and_key(metric=metric, granularity=granularity, **kwargs)
        pipe.hincrbyfloat(name=location, key=key, amount=amount )
        return pipe

