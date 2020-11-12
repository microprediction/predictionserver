from predictionserver.futureconventions.activityconventions import Activity, ActivityContext
from predictionserver.futureconventions.memoconventions import Memo
from predictionserver.serverhabits.sortedsethabits import SortedSetType, SortedSetNameGranularity, \
    SortedSetKeyGranularity
from predictionserver.servermixins.ownershipserver import OwnershipServer
from predictionserver.servermixins.memoserver import MemoServer
from collections import OrderedDict
from typing import Union
from copy import deepcopy


class SortedSetServer(MemoServer, OwnershipServer):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _authorize_sortedset_change(self, sortedset_type: SortedSetType, name_granularity: SortedSetNameGranularity,
                                    **kwargs):
        if name_granularity in [SortedSetNameGranularity.name, SortedSetNameGranularity.name_and_delay]:
            return self._authorize(name=kwargs['name'], write_key=kwargs['write_key'])
        elif name_granularity == SortedSetNameGranularity.write_key:
            return self.is_valid_key(write_key=kwargs['write_key'])
        elif name_granularity in [SortedSetNameGranularity.code, SortedSetNameGranularity.code_and_genus]:
            return self.shash(write_key=kwargs['write_key']) == kwargs['code']
        elif name_granularity in [SortedSetNameGranularity.memory]:
            return False  # System only
        else:
            return True

    def get_sortedset_values(self, sortedset_type: SortedSetType, name_granularity: SortedSetNameGranularity,
                             **name_kwargs):
        """ Retrieve entire sortedset_type """
        location = self.sortedset_location(sortedset_type=sortedset_type, name_granularity=name_granularity,
                                           **name_kwargs)
        raw = list(reversed(self.client.zrange(name=location, start=0, end=-1, withscores=True)))
        return OrderedDict([(k, float(v) if v is not None else 0.0) for k, v in raw])

    def add_sortedset_value(self, sortedset_type: SortedSetType,
                            name_granularity: SortedSetNameGranularity,
                            key_granularity: SortedSetKeyGranularity,
                            name_kwargs: dict,
                            key_kwargs: dict, value: float, write_key=None, verbose=False):
        return self._add_sortedset_value_implementation(sortedset_type=sortedset_type,
                                                        name_granularity=name_granularity,
                                                        key_granularity=key_granularity,
                                                        name_kwargs=name_kwargs,
                                                        write_key=write_key,
                                                        key_kwargs=key_kwargs, value=value, verbose=verbose)

    def multiply_sortedset(self, sortedset_type: SortedSetType, name_granularity: SortedSetNameGranularity,
                           name_kwargs: dict, weight: float, write_key=None, verbose=False):
        return self._multiply_sortedset_implementation(sortedset_type=sortedset_type, name_granularity=name_granularity,
                                                       name_kwargs=name_kwargs, weight=weight, write_key=write_key,
                                                       verbose=verbose)

    def delete_sortedset(self, sortedset_type: SortedSetType, name_granularity: SortedSetNameGranularity,
                         name_kwargs: dict,
                         write_key: str, verbose=False):
        """ If permission, confirmed delete of sortedset_type by user """
        return self._delete_sortedset_implementation(sortedset_type=sortedset_type, name_granularity=name_granularity,
                                                     name_kwargs=name_kwargs, write_key=write_key, verbose=verbose)

    # ------------------ #
    #   Implementation   #
    # ------------------ #

    def _delete_sortedset_implementation(self, sortedset_type: SortedSetType,
                                         name_granularity: SortedSetNameGranularity, name_kwargs: dict,
                                         verbose: Union[bool, int], write_key=None):
        """ If permission, confirmed delete of sortedset_type by user """
        write_key = write_key or name_kwargs.get('write_key')
        allowed = self._authorize_sortedset_change(sortedset_type=sortedset_type, name_granularity=name_granularity,
                                                   write_key=write_key, code=name_kwargs.get('code'),
                                                   name=name_kwargs.get('name'))
        location = self.sortedset_location(sortedset_type=sortedset_type, name_granularity=name_granularity,
                                           **name_kwargs)
        memo_kwargs = deepcopy(name_kwargs)
        memo_kwargs.update({'write_key': write_key})
        memo = Memo(activity=Activity.delete, sortedset_type=sortedset_type,
                    allowed=int(allowed), success=int(allowed), **memo_kwargs)
        if allowed:
            execution = self.client.delete(location)
            assert self.add_memo_as_owner_confirm(memo=memo, success=1, execution=execution)
        else:
            assert self.add_memo_as_owner_error(memo=memo, success=0, execution=0)
        return allowed if not verbose else memo.as_dict()

    def _add_sortedset_value_implementation(self, sortedset_type: SortedSetType,
                                            name_granularity: SortedSetNameGranularity,
                                            key_granularity: SortedSetKeyGranularity,
                                            name_kwargs: dict, key_kwargs: dict,
                                            verbose: Union[bool, int], value: float, write_key=None,
                                            context: ActivityContext = None):
        """ If permission, confirmed setting of sortedset_type value by user """
        write_key = write_key or name_kwargs.get('write_key') or key_kwargs.get('write_key')
        allowed = self._authorize_sortedset_change(sortedset_type=sortedset_type, name_granularity=name_granularity,
                                                   write_key=write_key, code=name_kwargs.get('code'),
                                                   name=name_kwargs.get('name'))
        location = self.sortedset_location(sortedset_type=sortedset_type, name_granularity=name_granularity,
                                           **name_kwargs)

        key = self.sortedset_key(key_granularity=key_granularity, **key_kwargs)
        memo_kwargs = deepcopy(name_kwargs)
        memo_kwargs.update({'write_key': write_key})
        memo = Memo(activity=Activity.add, sortedset_type=sortedset_type, context=context,
                    allowed=int(allowed), success=int(allowed), **memo_kwargs)
        if allowed:
            mapping = {key: value}
            execution = self.client.zadd(name=location, mapping=mapping)
            assert self.add_memo_as_owner_confirm(memo=memo, success=1, execution=execution)
        else:
            assert self.add_memo_as_owner_error(memo=memo, success=0, execution=0)
        return allowed if not verbose else memo.as_dict()

    def _multiply_sortedset_implementation(self, sortedset_type: SortedSetType,
                                           name_granularity: SortedSetNameGranularity,
                                           name_kwargs: dict, weight: float, write_key=None, verbose=False):
        """ If permission, multiply all values in sortedset_type by float multiplier """
        write_key = write_key or name_kwargs.get('write_key')
        allowed = self._authorize_sortedset_change(sortedset_type=sortedset_type, name_granularity=name_granularity,
                                                   write_key=write_key, code=name_kwargs.get('code'),
                                                   name=name_kwargs.get('name'))
        location = self.sortedset_location(sortedset_type=sortedset_type, name_granularity=name_granularity,
                                           **name_kwargs)
        memo_kwargs = deepcopy(name_kwargs)
        memo_kwargs.update({'write_key': write_key})
        memo = Memo(activity=Activity.multiply, sortedset_type=sortedset_type,
                    allowed=int(allowed), success=int(allowed), value=weight, **memo_kwargs)
        if allowed:
            import random
            temporary_key = 'temporary_' + ''.join([random.choice(['a', 'b', 'c']) for _ in range(20)])
            shrink_pipe = self.client.pipeline(transaction=True)
            shrink_pipe.zunionstore(dest=temporary_key, keys={location: weight})
            shrink_pipe.zunionstore(dest=location, keys={temporary_key: 1})
            shrink_pipe.expire(name=temporary_key, time=1)
            exec = shrink_pipe.execute()
            assert self.add_memo_as_owner_confirm(memo=memo, success=1, execution=1)
        else:
            assert self.add_memo_as_owner_error(memo=memo, success=0, execution=0)
        return allowed if not verbose else memo.as_dict()

    def _pipe_incr_sortedset_value_implementation(self, pipe, sortedset_type: SortedSetType,
                                                  name_granularity: SortedSetNameGranularity,
                                                  key_granularity: SortedSetKeyGranularity,
                                                  name_kwargs: dict, key_kwargs: dict,
                                                  amount=float):
        """ System use only """
        location = self.sortedset_location(sortedset_type=sortedset_type, name_granularity=name_granularity,
                                           **name_kwargs)
        key = self.sortedset_key(key_granularity=key_granularity, **key_kwargs)
        pipe = pipe.zincrby(name=location, value=key, amount=amount)
        return pipe
