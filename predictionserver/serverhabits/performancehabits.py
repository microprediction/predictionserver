
from predictionserver.futureconventions.performanceconventions import PerformanceConventions


class PerformanceHabits(PerformanceConventions):

    def __init__(self,**kwargs):
        super().__init__(**kwargs)
        self._PERFORMANCE_BACKWARD_COMPATIBLE = True
