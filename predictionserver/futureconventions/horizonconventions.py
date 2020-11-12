from predictionserver.futureconventions.sepconventions import SepConventions


class HorizonConventions:

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.DELAYS = [70, 310, 910, 3555]

    def ZDELAYS(self):
        return [self.DELAYS[0], self.DELAYS[-1]]  # Which delays to create z-streams for

    @staticmethod
    def horizon_name(name, delay):
        """ Convention is used for performance reports """
        return str(delay) + SepConventions.sep() + name

    @staticmethod
    def split_horizon_name(horizon):
        spl = horizon.split(SepConventions.sep())
        name = spl[1]
        delay = int(spl[0])
        return name, delay

    @staticmethod
    def split_horizon_names(self, horizons):
        names_delays = [self.split_horizon_name(key) for key in horizons]
        names = [n for n, _ in names_delays]
        delays = [d for _, d in names_delays]
        return names, delays

    @staticmethod
    def delay_from_horizon(horizon):
        return int(horizon.split(SepConventions.sep())[0])

    @staticmethod
    def name_from_horizon(horizon):
        return horizon.split(SepConventions.sep())[1]
