from predictionserver.futureconventions.autoconfigure import AutoConfigure
import requests
import logging


class BaseReader(AutoConfigure):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.base_url = 'https://api2.microprediction.org'

    def request_get_json(self, method, arg=None, data=None, throw=True):
        """ Call API with quiet fail """
        try:
            res = self.request_get_json_no_try(method=method, arg=arg, data=data)
        except ConnectionError as e:
            logging.warning('WARNING: ConnectionError attempting to get ' + method)
            if throw:
                raise e
        if res.status_code == 200:
            return res.json()

    def request_get_json_no_try(self, method, arg=None, data=None):
        if data is not None and arg is not None:
            res = requests.get(self.base_url + '/' + method + '/' + arg, data=data)
        elif data is not None and arg is None:
            res = requests.get(self.base_url + '/' + method, data=data)
        elif arg is not None:
            res = requests.get(self.base_url + '/' + method + '/' + arg)
        elif data is None and arg is None:
            res = requests.get(self.base_url + '/' + method)
        return res


if __name__ == '__main__':
    reader = BaseReader()
    print(reader.request_get_json(method='live', arg='cop.json'))
