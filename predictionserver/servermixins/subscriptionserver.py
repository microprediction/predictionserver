from predictionserver.serverhabits.subscriptionhabits import SubscriptionHabits
from predictionserver.servermixins.baseserver import BaseServer
from predictionserver.servermixins.ownershipserver import OwnershipServer
from predictionserver.servermixins.memoserver import MemoServer
from microconventions.type_conventions import NameList, Optional


class SubscriptionServer(SubscriptionHabits, BaseServer):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)


class StandaloneSubscriptionServer(SubscriptionServer, OwnershipServer, MemoServer):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def get_subscriptions(self, name):
        return self._get_subscriptions_implementation(name=name)

    def get_subscribers(self, name):
        return self._get_subscribers_implementation(name=name)

    def subscribe(self, name, write_key, source):
        """ Permissioned subscribe """
        return self._permissioned_subscribe_implementation(
            name=name, write_key=write_key, source=source
        )

    def msubscribe(self, name, write_key, sources):
        """ Permissioned subscribe to multiple sources """
        return self._permissioned_subscribe_implementation(
            name=name, write_key=write_key, sources=sources
        )

    def unsubscribe(self, name, write_key, source):
        return self._permissioned_unsubscribe_implementation(
            name=name, write_key=write_key, source=source
        )

    def munsubscribe(self, name, write_key, sources, delays=None):
        return self._permissioned_unsubscribe_implementation(
            name=name, write_key=write_key, sources=sources
        )

    def get_messages(self, name, write_key):
        """ Use key to open the mailbox """
        return self._get_messages_implementation(name=name, write_key=write_key)

    # ----------------- #
    #  Implementation   #
    # ----------------- #

    def _get_subscribers_implementation(self, name):
        return list(self.client.smembers(self.subscribers_name(name=name)))

    def _get_subscriptions_implementation(self, name):
        return list(self.client.smembers(self.subscriptions_name(name=name)))

    # --------------------------------------------------------------------------
    #            Public interface  (subscription)
    # --------------------------------------------------------------------------

    def _permissioned_subscribe_implementation(
            self,
            name,
            write_key,
            source=None,
            sources: Optional[NameList] = None
    ):
        """ Permissioned subscribe to one or more sources """
        if self._authorize(name=name, write_key=write_key):
            return self._subscribe_implementation(
                name=name, source=source, sources=sources
            )

    def _subscribe_implementation(self, name, source=None, sources=None):
        if source or sources:
            sources = sources or [source]
            the_pipe = self.client.pipeline()
            for _source in sources:
                the_pipe.sadd(self.subscribers_name(_source), name)
            the_pipe.sadd(self.subscriptions_name(name), *sources)
            exec = the_pipe.execute()
            return sum(exec) / 2
        else:
            return 0

    def _unsubscribe_pipe(self, pipe, name, source=None, sources=None):
        if source or sources:
            sources = sources or [source]
            for _source in sources:
                if _source is not None:
                    pipe.srem(self.subscribers_name(_source), name)
            if self._INSTANT_RECALL:
                pipe.hdel(self.messages_name(name), sources)
            pipe.srem(self.subscriptions_name(name), *sources)
        return pipe

    def _permissioned_unsubscribe_implementation(
            self, name, write_key, source=None, sources: Optional[NameList] = None
    ):
        """ Permissioned unsubscribe from one or more sources """
        if self._authorize(name=name, write_key=write_key):
            pipe = self.client.pipeline()
            pipe = self._unsubscribe_pipe(
                pipe=pipe, name=name, source=source, sources=sources
            )
            exec = pipe.execute()
            return sum(exec)
        else:
            return 0

    def _get_messages_implementation(self, name, write_key):
        if self._authorize(name=name, write_key=write_key):
            return self.client.hgetall(self.MESSAGES + name)


if __name__ == '__main__':
    from predictionserver.collider_config_private import (
        REDIZ_COLLIDER_CONFIG, EMBLOSSOM_MOTH
    )
    server = StandaloneSubscriptionServer(**REDIZ_COLLIDER_CONFIG)
    server.get_subscribers(name='die.json')
