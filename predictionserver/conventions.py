from predictionserver.futureconventions.attributeconventions import AttributeConventions
from predictionserver.futureconventions.horizonconventions import HorizonConventions
from predictionserver.futureconventions.autoconfigure import AutoConfigure
from predictionserver.futureconventions.keyconventions import KeyConventions
from predictionserver.futureconventions.laggedconventions import LaggedConventions
from predictionserver.futureconventions.memoconventions import MemoConventions
from predictionserver.futureconventions.metricconventions import MetricConventions
from predictionserver.futureconventions.scenarioconventions import ScenarioConventions
from predictionserver.futureconventions.performanceconventions import PerformanceConventions
from predictionserver.futureconventions.ratingconventions import RatingConventions


class Conventions(
    MemoConventions, AttributeConventions, HorizonConventions, AutoConfigure,
    KeyConventions, LaggedConventions, ScenarioConventions,
    PerformanceConventions, MetricConventions, RatingConventions
):

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
