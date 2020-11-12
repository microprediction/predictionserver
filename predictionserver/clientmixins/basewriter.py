from predictionserver.futureconventions.autoconfigure import AutoConfigure
from predictionserver.futureconventions.keyconventions import KeyConventions
import requests
import logging


class BaseWriter(AutoConfigure, KeyConventions):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def request_put(self, method, arg=None, params=None, throw=True):
        """ Canonical way to call API methods using requests library """
        try:
            if params is not None:
                res = requests.delete(self.base_url + '/' + method + '/' + arg, params=params)
            elif arg is not None:
                res = requests.delete(self.base_url + '/' + method + '/' + arg)
            elif params is None and arg is None:
                res = requests.delete(self.base_url + '/' + method)
            if res.status_code == 200:
                return res.json()
        except ConnectionError as e:
            logging.warning('WARNING: ConnectionError attempting to put ' + method)
            if throw:
                raise e

    def request_delete(self, method, arg=None, data=None, throw=True):
        """ Canonical way to call API methods using requests library """
        try:
            if data is not None:
                res = requests.delete(self.base_url + '/' + method + '/' + arg, data=data)
            elif arg is not None:
                res = requests.delete(self.base_url + '/' + method + '/' + arg)
            elif data is None and arg is None:
                res = requests.delete(self.base_url + '/' + method)
            if res.status_code == 200:
                return res.json()
        except ConnectionError as e:
            logging.warning('WARNING: ConnectionError attempting to delete ' + method)
            if throw:
                raise e


if __name__=='__main__':
    from predictionserver.collider_config_private import EMBLOSSOM_MOTH
    writer = BaseWriter()
    print(writer.request_delete(method='repository',arg=EMBLOSSOM_MOTH))