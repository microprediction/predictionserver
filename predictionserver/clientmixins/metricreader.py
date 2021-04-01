from predictionserver.futureconventions.metricconventions import (
    MetricConventions,
    MetricType,
    MetricGranularity,
)
from predictionserver.futureconventions.keyconventions import KeyConventions
from predictionserver.clientmixins.basereader import BaseReader
from typing import Union
import time


class MetricReader(MetricConventions, BaseReader):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def get_metric(
            self,
            metric: MetricType,
            granularity: Union[MetricGranularity, str] = None,
            **kwargs,
    ):
        """
            Supply all arguments implied by the AttributeGranularity, for example
            name and delay. Or see special cases below.
        """
        granularity = MetricGranularity[str(granularity)] if granularity \
            else self.metric_granularity_inference(**kwargs)
        kwargs['granularity'] = MetricGranularity[str(granularity)] if granularity \
            else self.metric_granularity_inference(**kwargs)
        return self.request_get_json(method='metric', arg=str(metric), data=kwargs)

    def get_metrics(
            self,
            metric: MetricType,
            granularity: Union[MetricGranularity, str] = None,
            **kwargs,
    ):
        """
            Supply all arguments implied by the AttributeGranularity, for example
            name and delay. Or see special cases below.
        """
        granularity = MetricGranularity[str(granularity)] if granularity \
            else self.metric_granularity_inference(**kwargs)
        kwargs['granularity'] = MetricGranularity[str(granularity)] if granularity else \
            self.metric_granularity_inference(**kwargs)
        return self.request_get_json(method='metrics', arg=str(metric), data=kwargs)

    # ----------------- #
    #    Special cases  #
    # ----------------- #

    def get_horizon_volume(self, name: str, delay: int) -> float:
        return self.get_metric(
            metric=MetricType.volume,
            granularity=MetricGranularity.name_and_delay,
            name=name,
            delay=delay,
        )

    def get_stream_latency(self, name: str) -> float:
        latest = self.get_metric(
            metric=MetricType.latest,
            granularity=MetricGranularity.name_and_delay,
            name=name,
        )
        return time.time() - float(latest)

    def get_stream_budget(self, name: str) -> float:
        return self.get_metric(
            metric=MetricType.budget,
            granularity=MetricGranularity.name,
            name=name,
        )

    def get_horizon_budget(self, name: str, delay: int) -> float:
        return self.get_metric(
            metric=MetricType.budget,
            granularity=MetricGranularity.name_and_delay,
            name=name,
            delay=delay,
        )

    def get_stream_count(self, name: str) -> float:
        """ Total number of data points ever published """
        return self.get_metric(
            metric=MetricType.count,
            granularity=MetricGranularity.name,
            name=name,
        )

    def get_owner_rating(self, code: str) -> float:
        """ Strength rating. Supply public identity. """
        return self.get_metric(
            metric=MetricType.rating,
            granularity=MetricGranularity.code,
            code=code,
        )


if __name__ == '__main__':
    from predictionserver.collider_config_private import ALBAHACA_MOLE
    code = KeyConventions.shash(write_key=ALBAHACA_MOLE)
    reader = MetricReader()
    print(MetricReader().get_stream_volume(name='cop.json'))
