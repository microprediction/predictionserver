from redis.client import list_or_args
from predictionserver.servermixins.baseserver import BaseServer
from collections import OrderedDict


# Ownership server has the limited responsibility of tracking the
# official owners of each stream
#
# Terminology:
#   - sponsor            refers to public identity (code, the shash of the write_key)
#   - owner/authority    refers to private identity, the write_key


class OwnershipServer(BaseServer):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        # ------------------- #
        #     System usage    #
        # ------------------- #

    def _set_ownership_implementation(self, name: str, write_key: str):
        """ Clobbers ... system use only """
        root = self._root_name(name)
        pipe = self.client.pipeline(transaction=True)
        pipe.hset(name=self._OWNERSHIP(), key=root, value=write_key)
        pipe.sadd(self._OWNERS(), write_key)
        return pipe.execute()

    def _delete_ownership_implementation(self, name: str, write_key: str):
        root = self._root_name(name)
        pipe = self.client.pipeline(transaction=True)
        pipe.hdel(self._OWNERSHIP(), root)
        pipe.srem(self._OWNERS(), write_key)
        execution = pipe.execute()
        return execution

    def get_names(self):
        return list(self.client.smembers(self._NAMES))

    def get_sponsors(self):
        ownership = self.client.hgetall(self._OWNERSHIP())
        obscured = [(name, self.animal_from_key(key)) for name, key in ownership.items()]
        obscured.sort(key=lambda t: len(t[1]))
        return OrderedDict(obscured)

    def _authorize(self, name, write_key):
        """ Check write_key against official records """
        # Only the stream owner can modify it
        return write_key == self._authority(name=name)

    def _mauthorize(self, names, write_keys):
        """ Parallel version of _authorize """
        authority = self._mauthority(names)
        assert len(names) == len(write_keys)
        comparison = [k == k1 for (k, k1) in zip(write_keys, authority)]
        return comparison

    def _authority(self, name):  # TODO: rename  stream_owner()
        """ Returns the write_key associated with name """
        root = self._root_name(name)
        return self.client.hget(self._OWNERSHIP(), root)

    def _mauthority(self, names, *args):  # TODO: rename  stream_owners()
        """ Parallel version of _authority """
        names = list_or_args(names, args)
        return self.client.hmget(self._OWNERSHIP(), *names)

    def stream_count(self):
        return self.client.scard(self._NAMES)

    def stream_exists(self, name):
        return self.client.sismember(name=self._NAMES, value=name)


if __name__ == '__main__':
    from predictionserver.collider_config_private import (
        REDIZ_COLLIDER_CONFIG, EMBLOSSOM_MOTH
    )

    server = OwnershipServer(**REDIZ_COLLIDER_CONFIG)
    assert server._authority(name='c5_ripple.json') == EMBLOSSOM_MOTH
    assert server._authorize(name='c5_ripple.json', write_key=EMBLOSSOM_MOTH)
