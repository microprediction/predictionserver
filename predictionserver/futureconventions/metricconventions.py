from predictionserver.futureconventions.sepconventions import SepConventions
from predictionserver.futureconventions.typeconventions import (
    GranularityEnum, StrEnum
)
from typing import Union

# Metrics are system incremented floats stored in hashes that track basic
# rolling statistics, and some advanced ones, by horizons and by participant

# Example:   metric::latest::code              stores most recent interactions by public identifier 'f107ab1e...'
#            metric::volume::name_and_delay    stores volumes keyed as '<streamname>|310'


class MetricType(StrEnum):
    latest = 0        # (latest record of activity, in epoch time)
    earliest = 1      # (earliest record of activity, in epoch time)
    count = 2         # (total number of data points)
    volume = 3        # (aggregate absolute value of rewards)
    rating = 4        # (rating estimate ... computed by ratings system)
    budget = 5        # (table stakes)


class MetricGranularity(GranularityEnum):
    """ Enumerates the way metrics are aggregated """
    write_key = 0
    code = 1
    name = 2
    name_and_delay = 3
    code_and_genus = 4


class MetricConventions:

    def __init__(self,**kwargs):
        super().__init__(**kwargs)
        self.METRICS = 'metrics' + SepConventions.sep()

    def metric_name_strict(self, metric: MetricType, granularity: MetricGranularity):
        """ Preferred way to reference name of hash storing a kind of metric """
        return self.METRICS + str(metric) + SepConventions.sep() + str(granularity)

    def metric_name(self, metric: Union[MetricType, str], code=None, name=None, delay=None, genus=None):
        """ Alternative method, where granularity is implicit """
        metric = MetricType[str(metric)]
        granularity = self.metric_granularity_inference(code=code, name=name, delay=delay, genus=genus)
        return self.metric_name_strict(granularity=granularity, metric=metric)

    @staticmethod
    def metric_granularity_inference(code=None, name=None, delay=None, genus=None, write_key=None):
        if genus is not None:
            return MetricGranularity.code_and_genus
        elif write_key is not None:
            return MetricGranularity.write_key
        elif code is not None:
            return MetricGranularity.code
        elif name is not None and delay is not None:
            return MetricGranularity.name_and_delay
        else:
            raise Exception('Cannot infer granularity of metric')