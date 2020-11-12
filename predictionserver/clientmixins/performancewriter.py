from predictionserver.futureconventions.autoconfigure import AutoConfigure
from predictionserver.clientmixins.basewriter import BaseWriter
from predictionserver.futureconventions.keyconventions import KeyConventions


class PerformanceWriter(AutoConfigure, BaseWriter, KeyConventions):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def delete_performance(self, write_key=None):
        """ Reset performance on *all* streams """
        write_key = write_key or self.own_write_key()
        return self.request_delete(method='performance', arg=write_key)


if __name__=='__main__':
    from predictionserver.collider_config_private import EMBLOSSOM_MOTH
    writer = BaseWriter()
    print(writer.request_delete(method='repository',arg=EMBLOSSOM_MOTH))