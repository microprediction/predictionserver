from predictionserver.habits import Habits
from predictionserver.servermixins.baseserver import BaseServer


class LegacyServer(BaseServer):

    def get_volumes_old(self):
        return self._descending_values(self.client.hgetall(name=self.VOLUMES))
