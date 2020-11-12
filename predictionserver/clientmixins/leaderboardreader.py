from predictionserver.futureconventions.keyconventions import KeyConventions
from predictionserver.futureconventions.leaderboardconventions import LeaderboardGranularity,\
    LeaderboardMemoryDescription, LeaderboardConventions
from predictionserver.clientmixins.basereader import BaseReader
from pprint import pprint
from typing import Union


class LeaderboardReader(KeyConventions,BaseReader, LeaderboardConventions):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def get_leaderboard(self, leaderboard_name:str, throw=True, count=1200, with_repos=False):
        """ Get any kind of leaderboard if you know the full name.
            See LeaderboardConventions for ways to construct the name, or use get_leaderboard_strict
        """
        data = {'count':count,'with_repos':int(with_repos)}
        return self.request_get_json(method='leaderboards', arg=leaderboard_name, data=data, throw=throw)

    # ------------- #
    #    Examples   #
    # ------------- #

    def get_leaderboard_strict(self, granularity:Union[LeaderboardGranularity,str], count=1200, with_repos=False,
                               throw=False, **kwargs):
        """ Retrieve any leaderboard, different style.
               Supply kwargs to popular granularity  (e.g.  name and delay,  or code and genus)
        """
        granularity = LeaderboardGranularity[str(granularity)]
        lb_name = self.leaderboard_name(granularity=granularity, **kwargs)
        return self.get_leaderboard(leaderboard_name=lb_name, throw=throw, count=count, with_repos=with_repos)

    def get_leaderboard_for_overall(self):
        lb_name = self.leaderboard_name_for_overall()
        return self.get_leaderboard(leaderboard_name=lb_name)

    def get_leaderboard_for_horizon(self, horizon:str=None, name:str=None, delay:str=None):
        lb_name = self.leaderboard_name_for_horizon(horizon=horizon, name=name, delay=delay)
        return self.get_leaderboard(leaderboard_name=lb_name)

    def get_leaderboard_for_stream(self, name:str):
        lb_name = self.leaderboard_name_for_stream(name=name)
        return self.get_leaderboard(leaderboard_name=lb_name)


if __name__ == '__main__':
    client = LeaderboardReader()
    print(client.get_leaderboard_for_overall())
