from predictionserver.servermixins.leaderboardserver import LeaderboardServer, LeaderboardGranularity
from predictionserver.servermixins.scenarioserver import ScenarioServer
from predictionserver.futureconventions.memoconventions import Memo, MemoCategory, Activity, ActivityContext
from predictionserver.servermixins.attributeserver import AttributeServer, AttributeGranularity, AttributeType
from predictionserver.serverhabits.memohabits import PrivateActor


class BankruptcyDaemon(LeaderboardServer, ScenarioServer, AttributeServer):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def admin_bankruptcy(self, with_report=False):
        """
            Force cancellations when owners of predictions are bankrupt
        """
        name = self.client.srandmember(self._NAMES)
        self.admin_bankruptcy_forced_cancellation(name=name, with_report=with_report)
        self.admin_bankruptcy_notification(name=name)

    def admin_bankruptcy_notification(self, name: str):
        """ Notify folks and set attribute so it only happens once """
        write_keys = list()
        for delay in self.DELAYS:
            write_keys.extend(self._get_sample_owners(name=name, delay=delay))
        for write_key in write_keys:
            if self.bankrupt(write_key):
                notified = self.get_attribute(attribute_type=AttributeType.update,
                                              granularity=AttributeGranularity.write_key,
                                              write_key=write_key) or False
                if not notified:
                    self.set_attribute(attribute_type=AttributeType.update,
                                       granularity=AttributeGranularity.write_key,
                                       write_key=write_key, value=1)
                    message = 'Your write_key is bankrupt. Consider topping it up with put_balance()'
                    self.add_owner_alert_message(write_key=write_key, message=message)

    def admin_bankruptcy_forced_cancellation(self, name, with_report):
        """
            Force cancellation of submissions for poorly performing bankrupt algorithms
        """
        discards = list()
        for delay in self.DELAYS:
            write_keys = self._get_sample_owners(name=name, delay=delay)
            leaderboard = self.get_leaderboard(
                granularity=LeaderboardGranularity.name_and_delay,
                count=10000,
                name=name,
                delay=delay,
                readable=False)
            losers = [key for key in write_keys if (self.shash(
                key) in leaderboard) and (leaderboard[self.shash(key)] < -1.0)]

            for write_key in losers:
                if self.bankrupt(write_key=write_key):
                    code = self.shash(write_key)
                    memo = Memo(
                        activity=Activity.cancel,
                        context=ActivityContext.bankruptcy,
                        name=name,
                        code=code,
                        write_key=write_key,
                        message='Initiating cancellation of submissions due to bankruptcy')
                    self.add_memo_as_owner_confirm(memo=memo)
                    self.cancel(name=name, write_key=write_key, delay=delay)
                    discards.append((name, write_key))
        report_data = dict(discards)
        report_memo = Memo(
            activity=Activity.daemon,
            context=ActivityContext.bankruptcy,
            count=len(report_data),
            data=report_data)
        self.add_memo_as_system_confirm(
            memo=report_memo,
            private_actor=PrivateActor.bankruptcy_daemon)
        return len(discards) if not with_report else report_memo.as_dict()
