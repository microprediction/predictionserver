from predictionserver.servermixins.baseserver import BaseServer
from predictionserver.serverhabits.laggedhabits import LaggedHabits
from pprint import pprint


class LaggedServer(LaggedHabits, BaseServer):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def get_delayed(self, name, delay=None, delays=None, to_float=True):
        return self._get_delayed_implementation(name=name, delay=delay, delays=delays, to_float=to_float)

    def get_lagged(self, name, start=0, end=None, count:int=None, to_float=True):
        return self._get_lagged_implementation(name, start=start, end=end, count=count, with_values=True,
                                               with_times=True, to_float=to_float)

    def get_lagged_values_and_times(self, name, start=0, end=None, count:int=None, to_float=True):
        times, values = self.get_lagged_times_and_values(name=name, start=start, end=end, count=count, to_float=to_float)
        return values, times

    def get_lagged_times_and_values(self, name, start=0, end=None, count:int=None, to_float=True):
        return self._get_lagged_implementation(name, start=start, end=end, count=count, with_values=True,
                                               with_times=True, to_float=to_float, separate=True)

    def get_lagged_values(self, name, start=0, end=None, count:int=None, to_float=True):
        return self._get_lagged_implementation(name, start=start, end=end, count=count, with_values=True,
                                               with_times=False, to_float=to_float)

    def get_lagged_times(self, name, start=0, end=None, count:int=None, to_float=True):
        return self._get_lagged_implementation(name, start=start, end=end, count=count, with_values=False,
                                               with_times=True, to_float=to_float)

    def get_history(self, name, max='+', min='-', count:int=None, populate=True, drop_expired=True):
        count = count or self._DEFAULT_HISTORY_COUNT
        return self._get_history_implementation(name=name, max=max, min=min, count=count, populate=populate,
                                                drop_expired=drop_expired)

    def _get_lagged_implementation(self, name, with_times, with_values, to_float, start=0, end=None, count:int=None, separate=False):
        """
        :param separate:    Do you want  lagged_values, lagged_times as two separate lists?
        :return:
        """
        count = count or self._DEFAULT_LAGGED_COUNT
        end = end or start + count - 1
        get_pipe = self.client.pipeline()
        if with_values:
            get_pipe.lrange(self.lagged_values_name(name), start=start, end=end)
        if with_times:
            get_pipe.lrange(self.lagged_times_name(name=name), start=start, end=end)
        res = get_pipe.execute()
        if with_values and with_times:
            raw_values = res[0]
            raw_times = res[1]
        elif with_values and not with_times:
            raw_values = res[0]
            raw_times = None
        elif with_times and not with_values:
            raw_times = res[0]
            raw_values = None

        if raw_values and to_float:
            try:
                values = self.to_float(raw_values)
            except:
                values = raw_values
        else:
            values = raw_values

        if raw_times and to_float:
            times = self.to_float(raw_times)
        else:
            times = raw_times

        if with_values and with_times:
            if separate:
                return times, values
            else:
                return list(zip(times, values))
        elif with_values and not with_times:
            return values
        elif with_times and not with_values:
            return times

    def _get_delayed_implementation(self, name, delay=None, delays=None, to_float=True):
        """ Get delayed values from one or more names """
        singular = delays is None
        delays = delays or [delay]
        full_names = [self.delayed_name(name=name, delay=delay) for delay in delays]
        delayed = self.client.mget(*full_names)
        if to_float:
            try:
                delayed = self.to_float(delayed)
            except:
                pass
        return delayed[0] if singular else delayed

    def _get_history_implementation(self, name, min, max, count, populate, drop_expired):
        """ Retrieve history, optionally replacing pointers with actual values  """
        history = self.client.xrevrange(name=self.HISTORY + name, min=min, max=max, count=count)
        if populate:
            has_pointers = any(self._POINTER in record for record in history)
            if has_pointers and populate:
                pointers = dict()
                for k, record in enumerate(history):
                    if self._POINTER in record:
                        pointers[k] = record[self._POINTER]

                values = self.client.mget(pointers)
                expired = list()
                for k, record in enumerate(history):
                    if k in pointers:
                        if values is not None:
                            fields = self.to_record(values[k])
                            record.update(fields)
                            expired.append(k)

                if drop_expired:
                    history = [h for j, h in enumerate(history) if not j in expired]
        return history


if __name__=='__main__':
    from predictionserver.collider_config_private import REDIZ_COLLIDER_CONFIG, FLATHAT_STOAT
    server = LaggedServer()
    server.connect(**REDIZ_COLLIDER_CONFIG)
    lagged = server.get_lagged(name='die.json')
    pprint(lagged[:6])
    print(len(lagged))
