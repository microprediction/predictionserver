from predictionserver.serverhabits.linkhabits import LinkHabits
from predictionserver.servermixins.baseserver import BaseServer
from predictionserver.servermixins.memoserver import MemoServer


class LinkServer(MemoServer):

    # --------------------------------------------------------------------------
    #            Public interface  (linking)
    # --------------------------------------------------------------------------

    def get_links(self, name, delay):
        return self._get_links(name=name, delay=delay)

    def get_links(self, name, delay=None, delays=None):
        assert not self.SEP in name, "Intent is to provide delay variable"
        return self._get_links_implementation(name=name, delay=delay, delays=delays)

    def get_backlinks(self, name):
        return self._get_backlinks_implementation(name=name)

    def set_link(self, name, write_key, delay, target=None, targets=None):
        """ Link from a delay to one or more targets """
        return self._set_link_implementation(name=name, write_key=write_key, delay=delay, target=target,
                                             targets=targets)

    def delete_link(self, name, delay, write_key, target):
        """ Permissioned removal of link (either party can do this) """
        return self._delete_link_implementation(name=name, delay=delay, write_key=write_key, target=target)

    # --------------------------------------------------------------------------
    #            Implementation  (linking)
    # --------------------------------------------------------------------------

    def _get_links(self, name, delay):
        return self.client.hgetall(self.links_name(name=name, delay=delay))

    def _get_links_implementation(self, name, delay=None, delays=None):
        """ Same but allows delays. Not sure why we need both.  """
        if delay is None and delays is None:
            delays = self.DELAYS
            singular = False
        else:
            singular = delays is None
            delays = delays or [delay]
        links = [self.client.hgetall(self.links_name(name=name, delay=delay)) for delay in delays]
        return links[0] if singular else links

    def _get_backlinks_implementation(self, name):
        """ Set of links pointing to name """
        return self.client.hgetall(self.backlinks_name(name=name))

    def _set_link_implementation(self, name, write_key, delay, target=None, targets=None):
        " Create link to possibly non-existent target(s) "
        # TODO: Maybe optimize with a beg for forgiveness patten to avoid two calls
        if targets is None:
            targets = [target]
        root = self._root_name(name)
        assert root == name, " Supply root name and a delay "
        target_root = self._root_name(target)
        assert target == target_root
        if self._authorize(name=root, write_key=write_key):
            link_pipe = self.client.pipeline()
            link_pipe.stream_exists(*targets)
            edge_weight = 1.0  # May change in the future
            for target in targets:
                link_pipe.hset(self.links_name(name=name, delay=delay), key=target, value=edge_weight)
                link_pipe.hset(self.backlinks_name(name=target), key=self.delayed_name(name=name, delay=delay),
                               value=edge_weight)
            exec = link_pipe.execute()
            return sum(exec) / 2
        else:
            return 0

    def _delete_link_implementation(self, name, delay, write_key, target):
        # Either party can unlink
        if self._authorize(name=name, write_key=write_key) or self._authorize(name=target, write_key=write_key):
            pipe = self.client.pipeline(transaction=True)
            pipe = self.__unlink(pipe=pipe, name=name, delay=delay, target=target)
            exec = pipe.execute()
            return exec

    def __unlink(self, pipe, name, delay, target):
        pipe.hdel(self.links_name(name, delay), target)
        pipe.hdel(self.backlinks_name(target), self.delayed_name(name=name, delay=delay))
        return pipe



class StandaloneLinkServer(LinkServer,BaseServer):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)


if __name__=='__main__':
    from predictionserver.collider_config_private import REDIZ_COLLIDER_CONFIG, FLATHAT_STOAT
    server = StandaloneLinkServer(**REDIZ_COLLIDER_CONFIG)
    print(server.links_name(name='die.json',delay=server.DELAYS[0]))