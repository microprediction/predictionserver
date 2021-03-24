from predictionserver.futureconventions.leaderboardconventions import LeaderboardGranularity
from predictionserver.futureconventions.memoconventions import Memo, MemoCategory, MemoGranularity
from predictionserver.futureconventions.activityconventions import Activity, ActivityContext, ActivityPublicity
from microconventions import Genus
from predictionserver.serverhabits.leaderboardhabits import LeaderboardHabits
from predictionserver.servermixins.ownershipserver import OwnershipServer
from pprint import pprint
from collections import OrderedDict
from typing import Union
import random
from microconventions import MicroConventions
from predictionserver.servermixins.memoserver import MemoServer


class LeaderboardServer(LeaderboardHabits, MemoServer):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def get_leaderboard(self,
                        granularity: Union[LeaderboardGranularity,
                                           str],
                        count=1200,
                        with_repos=False,
                        **kwargs):
        """ Retrieve any kind of leaderboard """
        leaderboard_granularity = LeaderboardGranularity[granularity] if isinstance(
            granularity, str) else granularity
        return self._get_leaderboard_implementation(
            granularity=leaderboard_granularity,
            count=count,
            with_repos=with_repos,
            **kwargs)

    def leaderboard_authorization(
            self,
            granularity: LeaderboardGranularity,
            **kwargs) -> bool:
        assert kwargs.get(
            'write_key'), ' Cannot authorize leaderboard operation without write_key'
        if 'name' in str(granularity):
            # Need the owner's write_key to reset leaderboard
            return self._authorize(name=kwargs['name'], write_key=kwargs['write_key'])
        elif 'code' in str(granularity):
            return kwargs['code'] == self.shash(kwargs['write_key'])
        else:
            return False

    def delete_leaderboard(self, granularity: LeaderboardGranularity, **kwargs):
        """ Sponsors can delete (i.e. reset) leaderboards that they control """
        granularity = LeaderboardGranularity[str(granularity)]
        allowed = self.leaderboard_authorization(granularity, **kwargs)
        memo = Memo(
            activity=Activity.delete,
            context=ActivityContext.leaderboard,
            granularity=granularity,
            allowed=allowed,
            **kwargs)
        if allowed:
            execution = self._delete_leaderboard_implementation(
                granularity=granularity, **kwargs)
            self.add_memo_as_owner_confirm(memo=memo, execution=execution)
            if execution == 0:
                self.add_memo_as_owner_warning(
                    memo=memo, execution=execution, message='Leaderboard did not exist')
        else:
            self.add_memo_as_owner_error(
                memo=memo,
                execution=0,
                message='Did not provide correct write_key associated with the leaderboard')
        return memo

    def scale_leaderboard(self, granularity: LeaderboardGranularity, **kwargs):
        granularity = LeaderboardGranularity[str(granularity)]
        allowed = self.leaderboard_authorization(granularity, **kwargs)
        memo = Memo(
            activity=Activity.multiply,
            context=ActivityContext.leaderboard,
            granularity=granularity,
            allowed=allowed,
            **kwargs)
        if allowed:
            execution = self._scale_leaderboard_implementation(
                granularity=granularity, **kwargs)
            self.add_memo_as_owner_confirm(memo=memo, execution=execution)
        else:
            self.add_memo_as_owner_error(
                memo=memo,
                execution=0,
                message='Did not provide correct write_key associated with the leaderboard')
        return memo

    # -------------- #
    # System usage   #
    # -------------- #

    def __incr_leaderboards_for_horizon(self, pipe, name, delay, payments, multiplier=1.0):
        leaderboard_names = self.leaderboard_names_to_update(
            name=name, delay=delay, sponsor=sponsor)
        for (recipient, amount) in payments.items():
            rescaled_amount = multiplier * float(amount)
            for lb in leaderboard_names:
                pipe = self.__incr_leaderboard(
                    pipe=pipe,
                    leaderboard_name=lb,
                    code=recipient,
                    amount=rescaled_amount)
        return pipe

    def __incr_leaderboard(self, pipe, leaderboard_name, code, amount):
        code = MicroConventions.code_from_code_or_key(code_or_key=code)
        amount = float(amount)
        pipe.zincrby(name=leaderboard_name, value=code, amount=amount)
        return pipe

    def _delete_leaderboard_implementation(self, granularity, name, **kwargs):
        return self.client.delete(
            self.leaderboard_name(
                leaderboard_granularity=granularity,
                name=name,
                **kwargs))

    def _get_leaderboard_implementation(
            self,
            granularity,
            count,
            readable=True,
            with_repos=False,
            **kwargs):
        leaderboard_name = self.leaderboard_name(granularity=granularity, **kwargs)
        return self._get_leaderboard_from_name(
            leaderboard_name=leaderboard_name,
            with_repos=with_repos,
            count=count,
            readable=readable)

    def _get_leaderboard_from_name(self, leaderboard_name, with_repos, count, readable):
        leaderboard = list(
            reversed(
                self.client.zrange(
                    name=leaderboard_name,
                    start=-count,
                    end=-1,
                    withscores=True)))
        if with_repos:
            return self._get_leaderboard_implementation_with_repos(leaderboard, readable)
        return OrderedDict([(MicroConventions.animal_from_code(code), score)
                           for code, score in leaderboard]) if readable else dict(leaderboard)

    def _get_leaderboard_implementation_with_repos(self, leaderboard, readable):
        hash_to_url_dict = self.client.hgetall(name=self._REPOS)
        return OrderedDict(
            [(MicroConventions.animal_from_code(code), (score, hash_to_url_dict.get(code, None)))
             for code, score in leaderboard]
        ) if readable else dict([(code, (score, hash_to_url_dict.get(code, None))) for code, score in leaderboard])

    def _copy_leaderboard(self, source, dest):
        self.client.unionstore(dest=dest, keys={source: 1})

    def _scale_leaderboard_implementation(
            self,
            granularity: LeaderboardGranularity,
            weight: float,
            **kwargs):
        leaderboard_name = self.leaderboard_name(granularity=granularity, **kwargs)
        if leaderboard_name is not None:
            temporary_key = 'temporary_' + \
                ''.join([random.choice(['a', 'b', 'c']) for _ in range(20)])
            shrink_pipe = self.client.pipeline(transaction=True)
            shrink_pipe.zunionstore(dest=temporary_key, keys={leaderboard_name: weight})
            shrink_pipe.zunionstore(dest=leaderboard_name, keys={temporary_key: 1})
            exec = shrink_pipe.execute()
            return {leaderboard_name: exec}

    # ----------------- #
    #   daemon tasks    #
    # ----------------- #

    def admin_shrinkage(self):
        """ Scale leaderboard scores towards zero """
        lb_name = self.client.rpop(self._SHRINK_QUEUE)
        kwargs = self.leaderboard_name_as_dict(leaderboard_name=lb_name)
        weight = 1. - self.SHRINKAGE
        execution = self._scale_leaderboard_implementation(weight=weight, **kwargs)
        memo = self.add_announcement_of_shrinkage(
            leaderboard_name=lb_name, execution=execution, weight=weight)
        return memo


class LeaderboardMigrationServer(LeaderboardServer, OwnershipServer, MemoServer):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def migrate_horizon_leaderboard(self, name, delay):
        old_lb_name = self.LEADERBOARD + self.horizon_name(name=name, delay=delay)
        new_lb_name = self.leaderboard_name_for_horizon(name=name, delay=delay)
        self.client.zunionstore(dest=new_lb_name, keys={old_lb_name: 1})
        new_retrieved = server.get_leaderboard(
            granularity=LeaderboardGranularity.name_and_delay, name=name, delay=delay)
        pass

    def migrate_stream_leaderboard(self, name):
        old_lb_name = self.LEADERBOARD + name
        new_lb_name = self.leaderboard_name_for_horizon(name=name)
        self.client.zunionstore(dest=new_lb_name, keys={old_lb_name: 1})
        new_retrieved = server.get_leaderboard(
            granularity=LeaderboardGranularity.name, name=name)
        pass

    def migrate_sponsored_leaderboard(self, name):
        """ Copy old leaderboard to new """
        owner = self._authority(name=name)
        sponsor = self.shash(write_key=owner)
        old_lb_name = self.old_custom_leaderboard_name(sponsor=sponsor, name=name)
        old_lb = self._get_leaderboard_from_name(
            leaderboard_name=old_lb_name, with_repos=False, count=5, readable=True)
        pprint(old_lb)
        genus = self.genus_from_name(name=name)
        new_leaderboard_name = self.leaderboard_name(
            granularity=LeaderboardGranularity.sponsor_and_genus, sponsor=sponsor, genus=genus)
        self.client.zunionstore(dest=new_leaderboard_name, keys={old_lb_name: 1})
        new_retrieved = server.get_leaderboard(
            granularity=LeaderboardGranularity.sponsor_and_genus,
            genus=genus,
            sponsor=sponsor,
            count=5)
        pprint(new_retrieved)

        # ---------------------------- #
        #    Backward compatibility    #
        # ---------------------------- #

    @staticmethod
    def old_lb_cat(name=None):
        if name is not None:
            if 'z1~' in name:
                return 'zscores_univariate'
            elif 'z2~' in name:
                return 'zcurves_bivariate'
            elif 'z3~' in name:
                return 'zcurves_trivariate'
            else:
                return 'regular'
        else:
            return 'all_streams'

    def old_custom_leaderboard_name(self, sponsor, name=None, dt=None):
        """ Names for leaderboards with a given sponsor
        :param animal:  str
        :param name:     str
        :param dt:       datetime
        :return:
        """

        def __init__(**kwargs):
            super().__init__(**kwargs)
            self.CUSTOM_LEADERBOARD = 'CUSTOM_LEADERBOARD' + self.SEP

        def lb_month(dt=None):
            return dt.isoformat()[:7] if dt is not None else 'all_time'

        sponsor = sponsor or 'overall'
        return self.SEP.join([self.CUSTOM_LEADERBOARD[:-2], sponsor,
                              self.old_lb_cat(name), lb_month(dt)]) + '.json'


if __name__ == '__main__':
    from predictionserver.collider_config_private import REDIZ_COLLIDER_CONFIG, EMBLOSSOM_MOTH
    server = LeaderboardMigrationServer(**REDIZ_COLLIDER_CONFIG)
    server.migrate_sponsored_leaderboard(name='btc_eur.json')
    pprint(
        server.get_leaderboard(
            granularity=LeaderboardGranularity.name_and_delay,
            name='die.json',
            delay=server.DELAYS[0]))
    server.migrate_stream_leaderboard(name='btc_eur.json')

    # Reset new leaderboard for september crypto
    sponsor = server.shash(EMBLOSSOM_MOTH)
