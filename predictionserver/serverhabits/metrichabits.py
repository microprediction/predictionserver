from predictionserver.futureconventions.metricconventions import MetricConventions, MetricGranularity, MetricType

# Metrics are system incremented values, typically float, stored in hashes.
# The name of the hash is specified by granularity only.
# The key into the hash is public. For example:

#     -  code
#     -  stream name
#     -  stream name and horizon


class MetricHabits(MetricConventions):

    def __init__(self,**kwargs):
        super().__init__(**kwargs)

    def _metric_key(self, granularity:MetricGranularity, **kwargs):
        return granularity.instance_name(**kwargs)

    def _metric_location_and_key(self, metric: MetricType, granularity: MetricGranularity, **kwargs):
        return self.metric_name_strict(metric=metric, granularity=granularity),\
               self._metric_key(granularity=granularity, **kwargs)





