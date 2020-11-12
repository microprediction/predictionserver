from predictionserver.futureconventions.keyconventions import KeyConventions
from predictionserver.clientmixins.basereader import BaseReader


class BalanceReader(KeyConventions,BaseReader):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def get_balance(self, write_key, throw=True):
        return self.request_get_json(method='balance',arg=write_key, throw=throw)


if __name__ == '__main__':
    from predictionserver.private.collider_config_private import ALBAHACA_MOLE
    client = BalanceReader()
    print(client.get_balance(write_key=ALBAHACA_MOLE))
