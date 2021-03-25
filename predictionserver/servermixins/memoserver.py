from predictionserver.futureconventions.memoconventions import (
    Memo, MemoCategory, MemoGranularity, Activity, ActivityContext, PublicActor
)
from predictionserver.servermixins.baseserver import BaseServer
from predictionserver.serverhabits.memohabits import MemoImplementation, PrivateActor
import json
from pprint import pprint


class MemoServer(BaseServer):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        # ------------------- #
        #   Getters           #
        # ------------------- #

    def get_memos(
            self, category: MemoCategory, granularity: MemoGranularity, **kwargs
    ):
        json_memos = self.get_memos_json(
            category=category, granularity=granularity, **kwargs)
        return [json.loads(jm) for jm in json_memos]

    def get_memos_json(
            self, category: MemoCategory, granularity: MemoGranularity, **kwargs
    ):
        location = self.memo_location(category=category, granularity=granularity, **kwargs)
        impl = self.MEMO_IMPLEMENTATIONS[category]
        if impl == MemoImplementation.redis_list:
            return self.client.lrange(name=location, start=0, end=-1)
        elif impl == MemoImplementation.redis_stream:
            return self.client.xrevrange(name=location, min='-', max='+', count=1000)

    def get_owner_memos(self, category: MemoCategory, write_key: str):
        return self.get_memos(
            category=category,
            granularity=MemoGranularity.write_key,
            write_key=write_key
        )

    def get_owner_errors(self, write_key: str):
        return self.get_owner_memos(category=MemoCategory.error, write_key=write_key)

    def get_owner_warnings(self, write_key: str):
        return self.get_owner_memos(category=MemoCategory.warning, write_key=write_key)

    def get_owner_confirms(self, write_key: str):
        return self.get_owner_memos(category=MemoCategory.confirm, write_key=write_key)

    def get_owner_transactions(self, write_key: str):
        return self.get_owner_memos(category=MemoCategory.transaction, write_key=write_key)

    def get_stream_transactions(self, name: str, delay: int):
        return self.get_memos(
            category=MemoCategory.transaction,
            granularity=MemoGranularity.name_and_delay,
            name=name,
            delay=delay
        )

    def get_system_memos(self, category: MemoCategory, private_actor: PrivateActor):
        return self.get_memos(
            category=category,
            granularity=MemoGranularity.private_actor,
            private_actor=private_actor
        )

    def get_public_memos(self, public_actor: PublicActor):
        return self.get_memos(
            category=MemoCategory.public,
            granularity=MemoGranularity.public_actor,
            public_actor=public_actor
        )

    def get_announcements(self):
        return self.get_public_memos(public_actor=PublicActor.announcer)

        # ------------------- #
        #   Write/delete      #
        # ------------------- #

    def add_memo_once(
            self, memo: Memo, category: MemoCategory, granularity: MemoGranularity
    ):
        memos = self.get_memos(
            category=category, granularity=granularity, **memo.as_dict()
        )
        memo_ids = [m['memo_id'] for m in memos]
        if memo['id'] not in memo_ids:
            self.add_memo(memo=memo, category=category, granularity=granularity)

    def add_memo(
            self, memo: Memo, category: MemoCategory, granularity: MemoGranularity
    ):
        """
             Remark: client facing functionality should only allow calling this when
                     granularity is write_key. Other uses are reserved for the system
        """
        return self.add_memos(
            memos=[memo],
            categories=[category],
            granularities=[granularity]
        )

    # Example...
    def add_announcement(self, message: str, url: str):
        """ Old style announcements are now 'alerts' from the announcer """
        memo = Memo(public_actor=PublicActor.announcer, message=message, url=url)
        return self.add_memo(
            memo=memo,
            category=MemoCategory.alert,
            granularity=MemoGranularity.public_actor
        )

    def delete_memos(
            self, category: MemoCategory, granularity: MemoGranularity, **kwargs
    ):
        """ Delete list/stream of memos
            Only possible when location is private
        """
        assert granularity in [
            MemoGranularity.write_key,
            MemoGranularity.private_actor], 'Cannot delete memos with provided granularity'
        location = self.memo_location(category=category, granularity=granularity, **kwargs)
        return self.client.delete(location)

    # --------------- #
    #  Special cases  #
    # --------------- #

    def delete_announcements(self):
        return self.delete_memos(
            category=MemoCategory.alert,
            granularity=MemoGranularity.alert,
            public_actor=PublicActor.announcer)

    def add_memo_as_owner_error(self, memo: Memo, success=0, allowed=None, execution=None):
        return self.execute_one(method=self._pipe_add_memo_as_owner_error, allowed=allowed,
                                memo=memo, success=success, execution=execution)

    def add_memo_as_owner_confirm(self, memo: Memo, success=1, allowed=1, execution=None):
        return self.execute_one(
            method=self._pipe_add_memo_as_owner_confirm,
            allowed=allowed,
            memo=memo,
            success=success,
            execution=execution)

    def add_memo_as_owner_warning(
            self,
            memo: Memo,
            success=0,
            allowed=None,
            execution=None):
        return self.execute_one(
            method=self._pipe_add_memo_as_owner_warning,
            allowed=allowed,
            memo=memo,
            success=success,
            execution=execution,
            message=None)

    def add_memo_as_owner_and_horizon_transaction(self, memo: Memo):
        return self.execute_one(
            method=self._pipe_add_memo_as_owner_and_horizon_transaction,
            memo=memo)

    def add_memo_as_owner_alert(self, memo: Memo):
        return self.execute_one(
            method=self._pipe_add_memo_as_owner_alert,
            memo=memo,
            write_key=None)

    # For example...
    def add_owner_alert_message(self, write_key: str, message: str, **kwargs):
        """ Append to private alert messages sent to owner """
        memo = Memo(message=message, write_key=write_key, **kwargs)
        self.add_memo_as_owner_alert(memo=memo)

    # ------------------------------- #
    #   Private system actor memos    #
    # ------------------------------- #

    def add_system_memo(
            self,
            memo: Memo,
            private_actor: PrivateActor,
            category: MemoCategory):
        self.execute_one(
            method=self.__add_system_memo,
            memo=memo,
            private_actor=private_actor,
            category=category)

    def add_memo_as_system_warning(self, memo: Memo, private_actor: PrivateActor):
        self.add_system_memo(
            memo=memo,
            private_actor=private_actor,
            category=MemoCategory.warning)

    def add_memo_as_system_error(self, memo: Memo, private_actor: PrivateActor):
        self.add_system_memo(
            memo=memo,
            private_actor=private_actor,
            category=MemoCategory.error)

    def add_memo_as_system_confirm(self, memo: Memo, private_actor: PrivateActor):
        self.add_system_memo(
            memo=memo,
            private_actor=private_actor,
            category=MemoCategory.confirm)

    # ------------------- #
    #   Implementation    #
    # ------------------- #

    def add_memos(
            self,
            memos: [Memo],
            categories: [MemoCategory],
            granularities: [MemoGranularity]):
        """ Push one or more memos into logs, broadcasting as needed
              memo must contain all quantities needed to resolve category instance name
        """
        pipe = self.client.pipeline()
        # Broadcast:
        max_len = max([len(memos), len(categories), len(granularities)])
        if len(categories) == 1:
            categories = [categories[0] for _ in range(max_len)]
        elif len(memos) == 1:
            memos = [memos[0] for _ in range(max_len)]
        if len(granularities) == 1:
            granularities = [granularities[0] for _ in range(max_len)]
        # Pipeline
        for memo, category, granularity in zip(memos, categories, granularities):
            pipe = self.__add_memo(
                pipe=pipe,
                memo=memo,
                category=category,
                granularity=granularity)
        return pipe.execute(raise_on_error=True)

    def _sanitize_dict(self, d: dict):
        for f1, f2 in Memo._SANITIZE_FIELDS.items():
            if f1 in d and d[f1] is not None:
                d[f2] = self.code_from_code_or_key(code_or_key=d[f1])
                del(d[f1])
        return d

    def __add_memo(
            self,
            pipe,
            memo: Memo,
            category: MemoCategory,
            granularity: MemoGranularity,
            **kwargs):
        imp_type = self.MEMO_IMPLEMENTATIONS[category]
        ttl = self.MEMO_TTLS[category]
        limit = self.MEMO_LIMITS[category]
        dmemo = memo.as_dict(cast_to_str=True, leave_out_none=True, flatten_data=True)
        location_new = self.memo_location(
            category=category, granularity=granularity, **dmemo)
        location_old = self.memo_location_old(memo=memo, category=category, **kwargs)
        dmemo = self._sanitize_dict(dmemo)
        if imp_type == MemoImplementation.redis_list:
            if location_old:
                pipe = self.__log_to_list(
                    pipe=pipe,
                    log_name=location_old,
                    ttl=ttl,
                    limit=limit,
                    **dmemo)
            pipe = self.__log_to_list(
                pipe=pipe,
                log_name=location_new,
                ttl=ttl,
                limit=limit,
                **dmemo)
        elif imp_type == MemoImplementation.redis_stream:
            raise NotImplementedError  #
        return pipe

    def _sanitize_list_log_entry(self, s: str):
        return s.replace(self.obscurity(), 'OBSCURE')

    def __log_to_list(self, pipe, log_name, ttl, limit, **kwargs):
        """ Append to list style log """
        pipe.lpush(log_name, self._sanitize_list_log_entry(json.dumps(kwargs)))
        pipe.expire(log_name, ttl)
        pipe.ltrim(log_name, start=0, end=limit)
        return pipe

    def _pipe_add_memo_as_owner_error(
            self,
            pipe,
            memo: Memo,
            success: int = 0,
            allowed: int = None,
            execution=None,
            message=None):
        """
          :return:  pipe
        """
        memo['success'] = success
        if message:
            memo['message'] = message
        if allowed is not None:
            memo['allowed'] = allowed
        if execution is not None:
            memo['execution'] = execution
        return self.__add_memo(
            pipe=pipe,
            memo=memo,
            granularity=MemoGranularity.write_key,
            category=MemoCategory.error
        )

    def _pipe_add_memo_as_owner_warning(
            self,
            pipe,
            memo: Memo,
            success=0,
            execution=None,
            allowed=None,
            message=None
    ):
        """
                 :return:  pipe
        """
        memo['success'] = success
        if message:
            memo['message'] = message
        if allowed is not None:
            memo['allowed'] = allowed
        if execution is not None:
            memo['execution'] = execution
        return self.__add_memo(
            pipe=pipe,
            memo=memo,
            granularity=MemoGranularity.write_key,
            category=MemoCategory.warning
        )

    def _pipe_add_memo_as_owner_confirm(
            self,
            pipe,
            memo: Memo,
            success=1,
            execution=None,
            allowed=1
    ):
        """
            :return:  pipe
        """
        memo['success'] = success
        memo['allowed'] = allowed
        if execution is not None:
            memo['execution'] = execution
        return self.__add_memo(
            pipe=pipe,
            memo=memo,
            granularity=MemoGranularity.write_key,
            category=MemoCategory.confirm
        )

    def _pipe_add_memo_as_owner_and_horizon_transaction(self, pipe, memo):
        """ Add to transaction log for owner, and also the horizon log
             :return:  pipe
        """
        pipe = self.__add_memo(
            pipe=pipe,
            memo=memo,
            category=MemoCategory.transaction,
            granularity=MemoGranularity.write_key
        )
        pipe = self.__add_memo(
            pipe=pipe,
            memo=memo,
            category=MemoCategory.transaction,
            granularity=MemoGranularity.name_and_delay
        )
        return pipe

    def _pipe_add_memo_as_owner_alert(self, pipe, memo, write_key=None):
        """ Private messages for owner of write_key """
        memo['write_key'] = write_key or memo['write_key']
        pipe = self.__add_memo(
            pipe=pipe,
            memo=memo,
            category=MemoCategory.alert,
            granularity=MemoGranularity.write_key
        )
        return pipe

    def __add_system_memo(
            self,
            pipe,
            memo: Memo,
            private_actor: PrivateActor,
            category: MemoCategory):
        """
        System messages are just memos with special actor identities that are
        impossible to guess
        """
        memo['private_actor'] = self.private_actor_key(private_actor=private_actor)
        pipe = self.__add_memo(
            pipe=pipe,
            memo=memo,
            category=category,
            granularity=MemoGranularity.private_actor)
        return pipe


if __name__ == '__main__':
    from predictionserver.collider_config_private import (
        REDIZ_COLLIDER_CONFIG, FLATHAT_STOAT
    )

    server = MemoServer()
    server.connect(**REDIZ_COLLIDER_CONFIG)

    # Create private alert and read it back
    memo1 = Memo(
        activity=Activity.set,
        context=ActivityContext.lagged,
        message='testing new confirms',
        write_key=FLATHAT_STOAT,
        data={'care': 7, 'dog': 13}
    )
    memo1['success'] = 1
    pprint(memo1.as_dict())
    server.add_memo_as_owner_confirm(memo=memo1)
    memos = server.get_memos(
        category=MemoCategory.confirm,
        granularity=MemoGranularity.write_key,
        write_key=FLATHAT_STOAT
    )
    pprint(memos)
