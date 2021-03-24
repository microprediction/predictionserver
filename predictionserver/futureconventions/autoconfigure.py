import requests
from getjson import getjson
import logging


class AutoConfigure:
    _REMOTELY_CONFIGURABLE = ['NUM_PREDICTIONS', 'MIN_BALANCE', 'DELAYS']

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.config_url = 'http://config.microprediction.org/config.json'
        self.failover_config_url = 'http://stableconfig.microprediction.org/config.json'

    @staticmethod
    def connected_to_internet(url='http://www.google.com/', timeout=5):
        try:
            _ = requests.get(url, timeout=timeout)
            return True
        except requests.ConnectionError:
            print("No internet connection available.")
        return False

    def auto_configure(self):
        """
        Set num_predictions and other properties to 'official' parameters
        at microprediction.org
        """
        try:
            remote_config = getjson(
                url=self.config_url,
                failover_url=self.failover_config_url
            )
        except Exception:
            if not self.connected_to_internet():
                raise Exception(
                    'Cannot initialize without internet access if parameters '
                    'are not supplied. Maybe check that your internet connection '
                    'is working.'
                )
            else:
                raise Exception(
                    'Could not initialize. Possibly due to slow internet. '
                    'Maybe try again in a couple of moments.'
                )
        if remote_config is not None:
            for parameter in self._REMOTELY_CONFIGURABLE:
                try:
                    setattr(self, parameter, remote_config[parameter.lower()])
                except AttributeError:
                    logging.warning(
                        f'Could not set {parameter} as it might be missing '
                        'from remote configuration.'
                    )
        else:
            raise Exception('Could not obtain remote configuration')
