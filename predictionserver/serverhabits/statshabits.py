from predictionserver.futureconventions.statsconventions import StatsConventions


class StatsHabits(StatsConventions):

    def __init__(self,**kwargs):
        super().__init__(**kwargs)


if __name__=='__main__':
    habits = StatsHabits()