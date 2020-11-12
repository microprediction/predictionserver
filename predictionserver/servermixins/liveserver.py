from predictionserver.futureconventions.typeconventions import ActivityContext, LoggingCategory, Activity, Memo, Attribute
from predictionserver.serverhabits.summarizinghabits import SummarizingHabits
from predictionserver.servermixins.baseserver import BaseServer
from predictionserver.servermixins.ownershipserver import OwnershipServer
from microconventions.type_conventions import NameList
from pprint import pprint
import json
from redis.client import list_or_args
from microconventions.leaderboard_conventions import LeaderboardVariety
from typing import Optional
from predictionserver.utilities import stem, has_nan, get_json_safe


class LiveServer(SummarizingHabits):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def get(self, name, as_json=False, **kwargs):
        """ Unified getter expecting prefixed name - used by web application """
        parts = name.split(self.SEP)
        kwargs.update({"as_json": as_json})
        if len(parts) == 1:
            data = self._get_implementation(name=name, **kwargs)
        else:
            data = self._get_prefixed_implementation(prefixed_name=name)

        if isinstance(data, set):
            data = list(set)
        return json.dumps(data) if as_json else data

    def mget(self, names: NameList, *args):
        names = list_or_args(names, args)
        return self._get_implementation(names=names)

    # ------------------- #
    #   Implementation    #
    # ------------------- #

    def _get_prefixed_implementation(self, prefixed_name):
        """ Interpret things like  delayed::15::air-pressure.json cdf::70::air-pressure.json etc """
        # Horrid mess. Should move this into conventions
        assert self.SEP in prefixed_name, "Expecting prefixed name with " + self.SEP
        parts = prefixed_name.split(self.SEP)
        if len(parts) == 2:
            ps = (parts[0] + self.SEP).lower()
            if ps == self.BACKLINKS:
                data = self.get_backlinks(name=parts[-1])
            if ps == self.SUMMARY:
                data = self.get_summary(name=parts[-1])
            elif ps == self.SUBSCRIPTIONS:
                data = self.get_subscriptions(name=parts[-1])
            elif ps == self.SUBSCRIBERS:
                data = self.get_subscribers(name=parts[-1])
            elif ps == self.LAGGED_VALUES:
                data = self.get_lagged_values(name=parts[-1])
            elif ps == self.CDF:
                nm, dly = self.split_horizon_name(prefixed_name)
                data = self.get_cdf(name=nm, delay=dly)
            elif ps == self.LAGGED:
                data = self.get_lagged(name=parts[-1])
            elif ps == self.LAGGED_TIMES:
                data = self.get_lagged_times(name=parts[-1])
            elif ps == self.ERRORS:
                data = self.get_errors(write_key=stem(parts[-1]))
            elif ps == self.PERFORMANCE:
                data = self.get_performance(write_key=stem(parts[-1]))
            elif ps == self.HISTORY:
                data = self.get_history(name=parts[-1])
            elif ps == self.BALANCE:
                data = self.get_balance(write_key=stem(parts[-1]))
            elif ps == self.BUDGETS:
                data = self.get_budget(name=parts[-1])
            elif ps == self.TRANSACTIONS:
                data = self.get_transactions(write_key=stem(parts[-1]))
            elif ps == self.LEADERBOARD:
                data = self.get_leaderboard(variety=LeaderboardVariety.name, name=parts[-1])
            else:
                data = None
        elif len(parts) == 3:
            ps = parts[0] + self.SEP
            if ps == self.DELAYED:
                data = self.get_delayed(name=parts[-1], delay=int(parts[1]), to_float=True)
            elif ps in [self._PREDICTIONS, self.PREDICTIONS]:
                data = self.get_predictions(name=parts[-1], delay=int(parts[1]))
            elif ps in [self._SAMPLES, self.SAMPLES]:
                data = self.get_samples(name=parts[-1], delay=int(parts[1]))
            elif ps == self.LINKS:
                data = self.get_links(name=parts[-1], delay=int(parts[1]))
            elif ps == self.TRANSACTIONS:
                data = self.get_transactions(write_key=parts[1], name=parts[2])
            elif ps == self.LEADERBOARD:
                # e.g. parts = 'leaderboard','name','blah.json'
                variety = LeaderboardVariety[parts[1]]
                lb_dict = self.leaderboard_name_as_dict(leaderboard_name=prefixed_name)
                data = self.get_leaderboard(variety=variety, **lb_dict)
            else:
                data = None
        else:
            data = None
        return data

    def _get_implementation(self, name: Optional[str] = None, names: Optional[NameList] = None, **nuissance):
        """ Retrieve value(s). No permission required. Write_keys or other extraneous arguments ignored. """
        plural = names is not None
        names = names or [name]
        res = self._pipelined_get(names=names)
        return res if plural else res[0]

    def _pipelined_get(self, names):
        """ Retrieve name values """
        # mget() may be faster but might be more prone to interrupt other processes? Not sure.
        if len(names):
            get_pipe = self.client.pipeline(transaction=True)
            for name in names:
                get_pipe.get(name=name)
            return get_pipe.execute()




if __name__=='__main__':
    from predictionserver.collider_config_private import REDIZ_COLLIDER_CONFIG, EMBLOSSOM_MOTH
    server = LiveServer()
    server.connect(**REDIZ_COLLIDER_CONFIG)
    value = server.get('die.json')
    print(value)