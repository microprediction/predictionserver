from predictionserver.futureconventions.activityconventions import Activity, ActivityContext
from predictionserver.futureconventions.memoconventions import Memo
from predictionserver.serverhabits.hashhabits import HashType, HashNameGranularity, HashKeyGranularity
from predictionserver.servermixins.ownershipserver import OwnershipServer
from predictionserver.servermixins.memoserver import MemoServer
import random
from collections import OrderedDict
from typing import Union
from copy import deepcopy


# Examples of hashes and keys
#
#   hash_type name                                                          key
#   obscure::hash_type::performance::write_key::87a6sf876sadf87            bluestream.json|310
#   obscure::hash_type::links::name::bluestream.json                       yellowstream.json
#   obscure::hash_type::active::write_key:987asf8798fdsa                   yellowstream.json|310


class HashServer(MemoServer, OwnershipServer):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _authorize_hash_change(self, hash_type: HashType, name_granularity: HashNameGranularity, name: str = None,
                               write_key: str = None, code=None):
        if name_granularity in [HashNameGranularity.name, HashNameGranularity.name_and_delay]:
            return self._authorize(name=name, write_key=write_key)
        elif name_granularity == HashNameGranularity.write_key:
            return self.is_valid_key(write_key=write_key)
        elif name_granularity == HashNameGranularity.code:
            return self.shash(write_key=write_key)==code
        else:
            return True

    def get_hash_value(self, hash_type: HashType, name_granularity: HashNameGranularity,
                       key_granularity: HashKeyGranularity,
                       name_kwargs: dict, key_kwargs: dict):
        """ Retrieve entire hash_type """
        location = self.hash_location(hash_type=hash_type, name_granularity=name_granularity, **name_kwargs)
        key = self.hash_key(key_granularity=key_granularity, **key_kwargs)
        raw = self.client.hget(name=location, key=key)
        return float(raw) if raw is not None else 0.0

    def get_hash_values(self, hash_type: HashType, name_granularity: HashNameGranularity, **name_kwargs):
        """ Retrieve entire hash_type """
        location = self.hash_location(hash_type=hash_type, name_granularity=name_granularity, **name_kwargs)
        raw = self.client.hgetall(name=location)
        return OrderedDict([(k, float(v) if v is not None else 0.0) for k, v in raw.items()])

    def set_hash_value(self, hash_type: HashType, name_granularity: HashNameGranularity,
                       key_granularity: HashKeyGranularity,
                       name_kwargs: dict,
                       key_kwargs: dict, value, verbose=False):
        return self._set_hash_value_implementation(hash_type=hash_type, name_granularity=name_granularity,
                                                   key_granularity=key_granularity,
                                                   name_kwargs=name_kwargs,
                                                   key_kwargs=key_kwargs, value=value, verbose=verbose)

    def delete_hash_value(self, hash_type: HashType, name_granularity: HashNameGranularity,
                                          key_granularity: HashKeyGranularity,
                                          name_kwargs: dict, key_kwargs: dict, verbose: bool,
                                          write_key=None):
        return self._delete_hash_value_implementation(hash_type=hash_type, name_granularity=name_granularity,
                                                      key_granularity=key_granularity, name_kwargs=name_kwargs,
                                                      key_kwargs=key_kwargs, verbose=verbose,
                                                      write_key=write_key)

    def multiply_hash(self, hash_type: HashType, name_granularity: HashNameGranularity,
                      name_kwargs: dict, weight: float, write_key=None, verbose=False):
        return self._multiply_hash_implementation(hash_type=hash_type, name_granularity=name_granularity,
                                                  name_kwargs=name_kwargs, weight=weight, write_key=write_key,
                                                  verbose=verbose)

    def delete_hash(self, hash_type: HashType, name_granularity: HashNameGranularity, name_kwargs: dict,
                    write_key: str, verbose=False):
        """ If permission, confirmed delete of hash_type by user """
        return self._delete_hash_implementation(hash_type=hash_type, name_granularity=name_granularity,
                                                name_kwargs=name_kwargs, write_key=write_key, verbose=verbose)

    def _delete_hash_implementation(self, hash_type: HashType, name_granularity: HashNameGranularity, name_kwargs: dict,
                                    verbose: Union[bool, int], write_key=None):
        """ If permission, confirmed delete of hash_type by user """
        write_key = write_key or name_kwargs.get('write_key')
        allowed = self._authorize_hash_change(hash_type=hash_type, name_granularity=name_granularity,
                                              write_key=write_key, code = name_kwargs.get('code'), name=name_kwargs.get('name'))
        location = self.hash_location(hash_type=hash_type, name_granularity=name_granularity, **name_kwargs)
        memo_kwargs = deepcopy(name_kwargs)
        memo_kwargs.update({'write_key': write_key})
        memo = Memo(activity=Activity.delete, hash_type=hash_type,
                    allowed=int(allowed), success=int(allowed), **memo_kwargs)
        if allowed:
            execution = self.client.delete(location)
            assert self.add_memo_as_owner_confirm(memo=memo, success=1, execution=execution)
        else:
            assert self.add_memo_as_owner_error(memo=memo, success=0, execution=0)
        return allowed if not verbose else memo.as_dict()

    def _set_hash_value_implementation(self, hash_type: HashType, name_granularity: HashNameGranularity,
                                       key_granularity: HashKeyGranularity,
                                       name_kwargs: dict, key_kwargs: dict,
                                       verbose: Union[bool, int], value=float, write_key=None,
                                       context: ActivityContext = None):
        """ If permission, confirmed setting of hash_type value by user """
        write_key = write_key or name_kwargs.get('write_key') or key_kwargs.get('write_key')
        allowed = self._authorize_hash_change(hash_type=hash_type, name_granularity=name_granularity,
                                              write_key=write_key, code = name_kwargs.get('code'), name= name_kwargs.get('name'))
        location = self.hash_location(hash_type=hash_type, name_granularity=name_granularity, **name_kwargs)

        key = self.hash_key(key_granularity=key_granularity, **key_kwargs)
        memo_kwargs = deepcopy(name_kwargs)
        memo_kwargs.update({'write_key': write_key})
        memo = Memo(activity=Activity.set, hash_type=hash_type, context=context,
                    allowed=int(allowed), success=int(allowed), **memo_kwargs)
        if allowed:
            execution = self.client.hset(name=location, key=key, value=value)
            assert self.add_memo_as_owner_confirm(memo=memo, success=1, execution=execution)
        else:
            assert self.add_memo_as_owner_error(memo=memo, success=0, execution=0)
        return allowed if not verbose else memo.as_dict()

    def _delete_hash_value_implementation(self, hash_type: HashType, name_granularity: HashNameGranularity,
                                          key_granularity: HashKeyGranularity,
                                          name_kwargs: dict, key_kwargs: dict, verbose: bool,
                                          write_key=None):
        """ If permission, confirmed setting of hash_type value by user  """
        write_key = write_key or name_kwargs.get('write_key')
        allowed = self._authorize_hash_change(hash_type=hash_type, name_granularity=name_granularity,
                                              write_key=write_key, code=name_kwargs.get('code'))
        location = self.hash_location(hash_type=hash_type, name_granularity=name_granularity, **name_kwargs)
        key = self.hash_key(key_granularity=key_granularity, **key_kwargs)
        memo_kwargs = deepcopy(name_kwargs)
        memo_kwargs.update({'write_key': write_key})
        memo = Memo(activity=Activity.delete, success=int(allowed), hash_type=hash_type,
                    allowed=int(allowed), **memo_kwargs)
        if allowed:
            execution = self.client.hdel(location, key)
            assert self.add_memo_as_owner_confirm(memo=memo, success=1, execution=execution)
        else:
            assert self.add_memo_as_owner_error(memo=memo, success=0, execution=0)
        return allowed if not verbose else memo.as_dict()

    def _multiply_hash_implementation(self, hash_type: HashType, name_granularity: HashNameGranularity,
                                      name_kwargs: dict, weight: float, write_key=None, verbose=False):
        """ If permission, multiply all values in hash_type by float multiplier """
        write_key = write_key or name_kwargs.get('write_key')
        allowed = self._authorize_hash_change(hash_type=hash_type, name_granularity=name_granularity,
                                              write_key=write_key, code=name_kwargs.get('code'), name=name_kwargs.get('name'))
        location = self.hash_location(hash_type=hash_type, name_granularity=name_granularity, **name_kwargs)
        memo_kwargs = deepcopy(name_kwargs)
        memo_kwargs.update({'write_key': write_key})
        memo = Memo(activity=Activity.multiply, hash_type=hash_type,
                    allowed=int(allowed), success=int(allowed), value=weight, **memo_kwargs)
        if allowed:
            mapping = self.client.hgetall(location)
            new_mapping = dict([(k, float(v) * weight) for k, v in mapping.items()])
            self.client.hset(name=location, mapping=new_mapping)
            assert self.add_memo_as_owner_confirm(memo=memo, success=1, execution=1)
        else:
            assert self.add_memo_as_owner_error(memo=memo, success=0, execution=0)
        return allowed if not verbose else memo.as_dict()

    def _pipe_incr_hash_value_implementation(self, pipe, hash_type: HashType, name_granularity: HashNameGranularity,
                                             key_granularity: HashKeyGranularity,
                                             name_kwargs: dict, key_kwargs: dict,
                                             amount=float):
        """ System use only """
        location = self.hash_location(hash_type=hash_type, name_granularity=name_granularity, **name_kwargs)
        key = self.hash_key(key_granularity=key_granularity, **key_kwargs)
        pipe = pipe.hincrbyfloat(name=location, key=key, amount=amount)
        return pipe
