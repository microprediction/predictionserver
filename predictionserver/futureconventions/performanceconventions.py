from predictionserver.futureconventions.sepconventions import SepConventions
from predictionserver.futureconventions.typeconventions import GranularityEnum

# Performances are hashes containing lists of running performance scores (keyed by horizon_name)
# They are incremented in a similar fashion to leaderboards, when data arrives.

# At present there is only one level of granularity in the name of the performance hash ... that is by owner.
#
# Performances can be reset or shrunk by the owner.
# Thus, the owner controls the performance - whereas they don't control leaderboards.


class PerformanceGranularity(GranularityEnum):
    write_key = 0


class PerformanceConventions:

    def __init__(self,**kwargs):
        super().__init__(**kwargs)
        self.PERFORMANCE = 'performance'+SepConventions.sep()

    def performance_name(self, granularity:PerformanceGranularity, **kwargs):
        """ The location of the performance hash """
        return self.PERFORMANCE + str(granularity) + SepConventions.sep() + granularity.instance_name(**kwargs)

    def performance_name_old(self, granularity:PerformanceGranularity, write_key):
        """ Backward compatibility """
        if granularity == PerformanceGranularity.write_key:
            return self.PERFORMANCE+write_key
        else:
            return None





if __name__=='__main__':
    from predictionserver.collider_config_private import REDIZ_COLLIDER_CONFIG, EMBLOSSOM_MOTH
    pc = PerformanceConventions()
    print(pc.performance_name(granularity=PerformanceGranularity.write_key, write_key=EMBLOSSOM_MOTH))