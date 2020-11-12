from predictionserver.clientmixins.basereader import BaseReader
from predictionserver.futureconventions.keyconventions import KeyConventions
from predictionserver.futureconventions.memoconventions import MemoConventions, MemoCategory
from typing import Union


class MemoReader(BaseReader, MemoConventions, KeyConventions):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def get_memos(self, category:Union[MemoCategory,str], write_key:str=None) -> [dict]:
        """ Backward compatible memo retrieval
             Supply write_key, typically, unless
        """
        new_memos = self.get_memos_new(category, write_key=write_key)
        old_memos = self.get_memos_old(category, write_key=write_key)
        return (new_memos or []) + (old_memos or [])

    def get_memos_new(self, category:Union[MemoCategory,str], throw=True, write_key=None):
        category = MemoCategory[str(category)]
        data = {'write_key':write_key} if write_key else None
        return self.request_get_json(method='memos', arg=str(category), data=data, throw=throw)

    # --------------------------- #
    #   Backward compatibility    #
    # --------------------------- #

    def get_memos_old(self, category:MemoCategory, write_key:str):
        method = str(category)+'s'
        if method in ['errors','warnings','confirms','transactions']:
            return self.request_get_json(method=method,arg=write_key)
        elif method in ['announcements']:
            return self.request_get_json(method=method)
        else:
            return None

    # --------------------------------- #
    #   Special cases as illustrations  #
    # --------------------------------- #

    def get_announcements(self):
        return self.get_memos(category=MemoCategory.announcement)

    def get_errors(self, write_key=None):
        write_key = write_key or self.own_write_key()
        return self.get_memos(category=MemoCategory.error, write_key=write_key)

    def get_warnings(self, write_key=None):
        write_key = write_key or self.own_write_key()
        return self.get_memos(category=MemoCategory.warning, write_key=write_key)

    def get_confirms(self, write_key=None):
        write_key = write_key or self.own_write_key()
        return self.get_memos(category=MemoCategory.confirm, write_key=write_key)

    def get_transactions(self, write_key=None):
        write_key = write_key or self.own_write_key()
        return self.get_memos(category=MemoCategory.transaction, write_key=write_key)


if __name__ == '__main__':
    from predictionserver.collider_config_private import ALBAHACA_MOLE
    client = MemoReader()
    print(client.get_memos(category=MemoCategory.confirm, write_key=ALBAHACA_MOLE))
    print(client.get_memos(category=MemoCategory.announcement, write_key=ALBAHACA_MOLE))

