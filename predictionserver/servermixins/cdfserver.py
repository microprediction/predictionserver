from predictionserver.servermixins.baseserver import BaseServer
from predictionserver.serverhabits.habits import CdfHabits
from microconventions.leaderboard_conventions import LeaderboardVariety
from predictionserver.servermixins.leaderboardserver import LeaderboardServer
from pprint import pprint
import numpy as np


class CdfServer(CdfHabits, LeaderboardServer):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def get_cdf(
            self,
            name: str,
            delay,
            values: [float] = None,
            top=10,
            min_balance=-
            50000000):
        """ Retrieve 'x' and 'y' values representing an approximate CDF
        :param values:   Abscissa for CDF ... if not supplied it will try to figure out something
        :param top:      Number of top participants to use
        :return:  {'x':[float],'y':[float]}
        """
        delay = int(delay)
        # TODO: Change to min_rating instead of min_balance
        if values is None:
            lagged_values = self.get_lagged_values(name=name)
            values = self.cdf_values(lagged_values=lagged_values, num=20, as_discrete=None)
        return self._get_cdf_implementation(
            name=name,
            delay=delay,
            values=values,
            top=top,
            min_balance=min_balance)

    # ------------------ #
    #   Implementation   #
    # ------------------ #

    def _get_cdf_implementation(self, name, delay, values, top, min_balance):
        """" Possibly we need to split logic into continuous and discrete cases :( """
        assert name == self._root_name(name), "get_cdf expects root name"
        assert delay in self.DELAYS, "delay is not a valid choice of delay"

        score_pipe = self.client.pipeline()
        num = self.client.zcard(name=self._predictions_name(name=name, delay=delay))
        lb = self._get_leaderboard_implementation(
            variety=LeaderboardVariety.name_and_delay,
            name=name,
            delay=delay,
            readable=False,
            count=top)
        included = [write_key for write_key, balance in lb.items() if balance > min_balance]
        if num:
            h = min(0.1, max(5.0 / num, 0.00001)) * max([abs(v) for v in values] + [1.0])
            for value in values:
                score_pipe.zrevrangebyscore(
                    name=self._predictions_name(
                        name=name,
                        delay=delay),
                    max=value,
                    min=value - h,
                    start=0,
                    num=5,
                    withscores=False)
                score_pipe.zrangebyscore(
                    name=self._predictions_name(
                        name=name,
                        delay=delay),
                    min=value,
                    max=value + h,
                    start=0,
                    num=5,
                    withscores=False)

            execut = score_pipe.execute()
            execut_combined = self.chunker(execut, n=len(values))
            execut_merged = [lst[0] + lst[1] for lst in execut_combined]
            prtcls = [
                self._zmean_scenarios_percentile(
                    percentile_scenarios=ex,
                    included_codes=included) if ex else np.NaN for ex in execut_merged]
            valid = [
                (v, p) for v, p in zip(
                    values, prtcls) if not np.isnan(p) and abs(
                    p - 0.5) > 1e-6]

            # Make CDF monotone using avg of min and max envelopes running from each
            # direction
            ys1 = [p for v, p in valid]
            ys2 = list(reversed([p for v, p in valid]))
            xs = [v for v, p in valid]
            if ys1:
                ys_monotone_1 = list(np.maximum.accumulate(np.array(ys1)))
                ys_monotone_2 = list(np.minimum.accumulate(np.array(ys2)))
                ys_monotone = [0.5 * (y1 + y2)
                               for y1, y2 in zip(ys_monotone_1, reversed(ys_monotone_2))]
                return {"x": xs, "y": ys_monotone}
            else:
                return {"x": xs, "y": ys1}
        else:
            return {"message": "No predictions."}

    def _get_scenarios_implementation(self, name, write_key, delay, cursor=0):
        """ Charge for this! Not encouraged as it should not be necessary, and it is inefficient to get scenarios back from the collective zset """
        assert name == self._root_name(name)
        if self.is_valid_key(write_key) and delay in self.DELAYS:
            cursor, items = self.client.zscan(name=self._predictions_name(
                name=name, delay=delay), cursor=cursor, match='*' + write_key + '*', count=self.num_predictions)
            return {"cursor": cursor, "scenarios": dict(items)}


class StandaloneCdfServer(CdfServer, LeaderboardServer, BaseServer):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)


if __name__ == '__main__':
    from predictionserver.collider_config_private import REDIZ_COLLIDER_CONFIG, FLATHAT_STOAT
    server = StandaloneCdfServer(**REDIZ_COLLIDER_CONFIG)
    lb = server.get_leaderboard(
        granularity=LeaderboardVariety.name_and_delay,
        name='die.json',
        delay=server.DELAYS[0])
    pprint(lb)
