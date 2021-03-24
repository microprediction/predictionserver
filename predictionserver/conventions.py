from micropredictionserver.futureconventions.attributeconventions import AttributeConventions
from micropredictionserver.futureconventions.horizonconventions import HorizonConventions
from micropredictionserver.futureconventions.autoconfigure import AutoConfigure
from micropredictionserver.futureconventions.keyconventions import KeyConventions
from micropredictionserver.futureconventions.laggedconventions import LaggedConventions
from micropredictionserver.futureconventions.memoconventions import MemoConventions
from micropredictionserver.futureconventions.metricconventions import MetricConventions
from micropredictionserver.futureconventions.scenarioconventions import ScenarioConventions
from micropredictionserver.futureconventions.performanceconventions import PerformanceConventions
from micropredictionserver.futureconventions.ratingconventions import RatingConventions, RatingGranularity


class Conventions(MemoConventions, AttributeConventions, HorizonConventions,
                  AutoConfigure, KeyConventions, LaggedConventions, ScenarioConventions,
                  PerformanceConventions, MetricConventions, RatingConventions):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.base_url = 'http://api.microprediction.org'
        self.failover_base_url = 'http://stableapi.microprediction.org'


if __name__ == '__main__':
    c = Conventions()
    c.auto_configure()
    print(c.base_url)
    print(c.DELAYS)
    print(c.ZDELAYS())
