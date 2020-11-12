from predictionserver.futureconventions.leaderboardconventions import LeaderboardConventions, LeaderboardGranularity, LeaderboardMemoryDescription
from predictionserver.futureconventions.typeconventions import Genus
from predictionserver.serverhabits.obscurityhabits import ObscurityHabits
from pprint import pprint

# Remark: Most leaderboard conventions are public
#         (So see leaderboardconventions to understand the enumeration of leaderboards.)
#
# Leaderboards are shrunk stochastically by a daemon every now and then.
# The daemon reads from the shrink queue


class LeaderboardHabits(LeaderboardConventions, ObscurityHabits):

    def __init__(self,**kwargs):
        super().__init__(**kwargs)
        self._LEADERBOARD_TTL = 14 * 24 * 60 * 60  # How long to keep leaderboards for inactive streams

    def _SHRINK_QUEUE(self):
        raise Exception('You want shink_queue_location() ')

    def shrink_queue_location(self):
        return self.obscurity() + "shrink_queue"

    def leaderboard_names_to_update(self, name, code, delay):
        """ Leaderboards that update when data arrives """
        genus = Genus.from_name(name=name)
        memory_boards = [self.leaderboard_name_strict(granularity=LeaderboardGranularity.memory, memory=memory) for memory in self.LEADERBOARD_MEMORIES.values()]
        medium_memory = self.LEADERBOARD_MEMORIES[LeaderboardMemoryDescription.medium]
        stream_boards = [self.leaderboard_name_strict(granularity=granularity, name=name, code=code,memory=medium_memory, delay=delay, genus=genus) for granularity in LeaderboardGranularity]
        return sorted(list(set(stream_boards + memory_boards)))



if __name__=='__main__':
    lbh = LeaderboardHabits()
    lb_names = lbh.leaderboard_names_to_update(name='bill', code='lkjasf897asdf9', delay=72)
    lb_dicts = [lbh.leaderboard_name_as_dict(name) for name in lb_names]
    pprint((lb_dicts))