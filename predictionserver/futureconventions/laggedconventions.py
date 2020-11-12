from predictionserver.futureconventions.sepconventions import SepConventions


class LaggedConventions:

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.LAGGED = "lagged" + SepConventions.sep()
        self.LAGGED_VALUES = "lagged_values" + SepConventions.sep()
        self.LAGGED_TIMES = "lagged_times" + SepConventions.sep()

    def lagged_values_name(self, name):
        return self.LAGGED_VALUES + name

    def lagged_times_name(self, name):
        return self.LAGGED_TIMES + name
