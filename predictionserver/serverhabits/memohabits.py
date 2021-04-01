from predictionserver.futureconventions.memoconventions import (
    MemoCategory, Memo, MemoGranularity, MemoConventions
)
from predictionserver.futureconventions.sepconventions import SepConventions
from predictionserver.futureconventions.typeconventions import StrEnum
from predictionserver.serverhabits.obscurityhabits import ObscurityHabits
from predictionserver.futureconventions.keyconventions import KeyConventions
import os


class MemoImplementation(StrEnum):
    redis_stream = 0
    redis_list = 1


class PrivateActor(StrEnum):
    promise_daemon = 0
    bankruptcy_daemon = 1
    garbage_daemon = 2
    shrinkage_daemon = 3


class MemoHabits(MemoConventions, ObscurityHabits):

    # Consider making these public

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.MEMO_TTLS = {
            MemoCategory.warning: 12 * 60 * 60,
            MemoCategory.error: 24 * 60 * 60,
            MemoCategory.confirm: 12 * 60 * 60,
            MemoCategory.transaction: 24 * 60 * 60,
            MemoCategory.alert: 24 * 60 * 60 * 30
        }
        self.MEMO_LIMITS = {
            MemoCategory.warning: 50,
            MemoCategory.error: 50,
            MemoCategory.confirm: 500,
            MemoCategory.transaction: 500,
            MemoCategory.alert: 5
        }
        # lkj
        self.MEMO_IMPLEMENTATIONS = {
            MemoCategory.warning: MemoImplementation.redis_list,
            MemoCategory.error: MemoImplementation.redis_list,
            MemoCategory.confirm: MemoImplementation.redis_list,
            MemoCategory.alert: MemoImplementation.redis_list,
            MemoCategory.transaction: MemoImplementation.redis_list
        }

        # Backward compatibility ... TODO: refactor these out.
        self.WARNINGS_TTL = self.MEMO_TTLS[MemoCategory.warning]
        self.WARNINGS_LIMIT = 1000
        self.CONFIRMS_TTL = self.MEMO_TTLS[MemoCategory.confirm]
        self.ERROR_TTL = self.MEMO_TTLS[MemoCategory.error]
        self.ERROR_LIMIT = self.MEMO_LIMITS[MemoCategory.error]
        self.CONFIRMS_LIMIT = self.MEMO_LIMITS[MemoCategory.confirm]
        self.TRANSACTIONS_LIMIT = self.MEMO_LIMITS[MemoCategory.transaction]
        # How long to keep transactions stream for inactive write_keys
        self.TRANSACTIONS_TTL = self.MEMO_TTLS[MemoCategory.transaction]

    def private_actor_key(self, private_actor: PrivateActor):
        """ Fake obscure key used for system messages """
        return KeyConventions.shash(str(private_actor) + self.obscurity())

    def is_valid_private_actor_key(self, key):
        return key in [
            self.private_actor_key(private_actor=pa) for pa in PrivateActor
        ]

    def _MEMOS(self):
        raise Exception('You want _memos()')

    def ___memos(self):
        return self.obscurity() + 'memos' + SepConventions.sep()

    def memo_location(
            self, category: MemoCategory, granularity: MemoGranularity, **kwargs
    ):
        """ Determine name of stream or list, such as memos::warning::write_key
            These are mostly straightforward, but for convenience we allow a typed
            private actor to be supplied
        """
        private_actor = kwargs.get('private_actor')
        if private_actor is not None:
            kwargs.pop('private_actor')
            if isinstance(private_actor, PrivateActor):
                private_actor = self.private_actor_key(private_actor=private_actor)
            else:
                assert self.is_valid_private_actor_key(
                    private_actor), 'system actor key is invalid'
            kwargs['private_actor'] = private_actor
        return self.___memos() + str(category) + SepConventions.sep() + \
            granularity.instance_name(**kwargs)

    def _transactions_location_old(self, memo: Memo):
        """ Convention for names of transactions records """
        write_key = memo['write_key']
        name = memo.get('name')
        delay = memo.get('delay')
        delay = None if delay is None else str(delay)
        key_stem = None if write_key is None else os.path.splitext(write_key)[0]
        name_stem = None if name is None else os.path.splitext(name)[0]
        tail = SepConventions.sep().join(
            [s for s in [key_stem, delay, name_stem] if s is not None])
        return 'transactions' + SepConventions.sep() + tail + '.json'

    def memo_location_old(self, memo: Memo, category: MemoCategory):
        if category == MemoCategory.transaction:
            return self._transactions_location_old(memo=memo)
        elif category in [MemoCategory.error, MemoCategory.warning, MemoCategory.confirm]:
            return str(category) + 's' + SepConventions.sep() + memo['write_key'] + '.json'
        else:
            return None
