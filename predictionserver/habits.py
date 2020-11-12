from predictionserver.futureconventions.autoconfigure import AutoConfigure
from predictionserver.serverhabits.balancehabits import BalanceHabits
from predictionserver.serverhabits.keyhabits import KeyHabits
from predictionserver.serverhabits.attributehabits import AttributeHabits
from predictionserver.serverhabits.horizonhabits import HorizonHabits
from predictionserver.serverhabits.naminghabits import NamingHabits
from predictionserver.serverhabits.laggedhabits import LaggedHabits
from predictionserver.serverhabits.memohabits import MemoHabits
from predictionserver.serverhabits.scenariohabits import ScenarioHabits
from predictionserver.serverhabits.ownershiphabits import OwnershipHabits
from predictionserver.serverhabits.plottinghabits import PlottingHabits
from predictionserver.serverhabits.statshabits import StatsHabits
from predictionserver.futureconventions.zcurveconventions import ZCurveConventions
from predictionserver.serverhabits.metrichabits import MetricHabits
from predictionserver.serverhabits.hashhabits import HashHabits
from predictionserver.serverhabits.sortedsethabits import SortedSetHabits


class Habits(PlottingHabits, LaggedHabits, HorizonHabits, ScenarioHabits, StatsHabits, NamingHabits,
             AttributeHabits, MetricHabits, HashHabits, SortedSetHabits,
             MemoHabits, BalanceHabits, OwnershipHabits, KeyHabits, AutoConfigure, ZCurveConventions):

    def __init__(self,**kwargs):
        super().__init__(**kwargs)


if __name__=='__main__':
    habits = Habits()
    print(habits.BALANCE)
    print(habits._OWNERS()) # <--- will fail