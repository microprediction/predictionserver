from predictionserver.futureconventions.laggedconventions import LaggedConventions


class LaggedHabits(LaggedConventions):

    def __init__(self,**kwargs):
        super().__init__(**kwargs)
        self._DEFAULT_LAGGED_COUNT = 2000   # How many lagged values to return (as compared to how many to store)
        self._DEFAULT_HISTORY_COUNT = 50   # How many historical valules to return

    def _POINTER(self):
        raise Exception('You want _history_pointer()')

    def history_pointer(self):
        return self.obscurity() + "pointer"

