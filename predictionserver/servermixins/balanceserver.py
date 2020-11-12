from predictionserver.futureconventions.activityconventions import ActivityContext
from predictionserver.futureconventions.memoconventions import Memo, MemoCategory, Activity
from predictionserver.servermixins.memoserver import MemoServer
from predictionserver.servermixins.metricserver import MetricServer


class BalanceServer(MemoServer, MetricServer):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def get_balance(self, write_key):
        return float(self.client.hget(name=self._BALANCES(), key=write_key) or 0)

    def put_balance(self, source_write_key, recipient_write_key, amount=None, verbose=False):
        return self._put_balance_implementation(source_write_key=source_write_key,
                                                recipient_write_key=recipient_write_key,
                                                amount=amount, verbose=verbose)

    def get_reserve(self):
        return float(self.client.hget(self._BALANCES, self._RESERVE()) or 0)

    #-------------- #
    # System usage  #
    # ------------- #

    def __update_balance(self, pipe, write_key:str, amount:float):
        pipe.hincrbyfloat(name=self._BALANCES, key=write_key, amount=amount)
        return pipe

    def _put_balance_implementation(self, source_write_key, recipient_write_key, amount, verbose=False):
        """ Debit and credit write_keys """
        allowed = self.key_permission_to_receive(write_key=recipient_write_key) and self.key_permission_to_submit(source_write_key)
        give_memo = Memo(activity=Activity.give, context=ActivityContext.balance,
                         counterparty_code=self.shash(recipient_write_key), allowed=allowed, success=allowed)
        receive_memo = Memo(activity=Activity.receive, context=ActivityContext.balance,
                            counterparty_code=self.shash(source_write_key), allowed=allowed, success=allowed)
        if allowed:
            recipient_balance = self.get_balance(write_key=recipient_write_key)
            if recipient_balance < -1.0:
                source_distance = self.distance_to_bankruptcy(write_key=source_write_key)
                maximum_to_receive = -recipient_balance  # Cannot make a balance positive
                maximum_to_give = max(0,source_distance)
                if amount is None:
                    amount = maximum_to_give
                given = min([maximum_to_give, maximum_to_receive, amount])
            else:
                given = 0
            allowed = given>0

            if not(allowed):
                give_memo['category']=MemoCategory.error
                self._error(give_memo.as_dict())
            else:
                pipe = self.client.pipeline(transaction=True)
                pipe.hincrbyfloat(name=self._BALANCES, key=source_write_key, amount=-given)
                pipe.hincrbyfloat(name=self._BALANCES, key=recipient_write_key, amount=given)
                execution = all([ abs(ex) for ex in pipe.execute()])
                success = execution
                give_memo.update({'execution':execution,'success':success,'data':{'given':given}})
                receive_memo.update({'execution':execution,'success':success,'data':{'given':given}})
                self._confirm(write_key=source_write_key,**give_memo.as_dict())
                self._confirm(write_key=recipient_write_key, **receive_memo.as_dict())

        return give_memo['success'] if not verbose else give_memo.as_dict()



if __name__=='__main__':
    from predictionserver.collider_config_private import REDIZ_COLLIDER_CONFIG, FLATHAT_STOAT
    server = BalanceServer()
    server.connect(**REDIZ_COLLIDER_CONFIG)
    print(server.get_balance(write_key=FLATHAT_STOAT))
    print(server.get_reserve())