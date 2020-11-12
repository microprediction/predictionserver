from predictionserver.futureconventions.hashconventions import HashType, HashConventions, HashNameGranularity,HashKeyGranularity
from predictionserver.futureconventions.sepconventions import SepConventions
from predictionserver.serverhabits.obscurityhabits import ObscurityHabits
from typing import Union


# Examples of hash hashs and keys
#
#       hash name                                                      key
#   obscure::hash::performance::write_key::87a6sf876sadf87            bluestream.json|310
#   obscure::hash::links::name::bluestream.json                       yellowstream.json
#   obscure::hash::active::write_key:987asf8798fdsa                   yellowstream.json|310


class HashHabits(HashConventions, ObscurityHabits):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._HASH = 'hash'

    def hash_location(self, hash_type: Union[HashType, str], name_granularity: Union[HashNameGranularity, str],**kwargs):
        """ Determine hash hash hash name, such as  hash::email::write_key    """
        hash_type = HashType[str(hash_type)]
        name_granularity = HashNameGranularity[str(name_granularity)]
        instance = name_granularity.instance_name(**kwargs)
        return self.obscurity() + SepConventions.sep().join([self._HASH, str(hash_type), str(name_granularity), instance])
      
    def hash_key(self, key_granularity:Union[HashKeyGranularity,str], **kwargs):
        key_granularity = HashKeyGranularity[str(key_granularity)]
        return key_granularity.instance_name(**kwargs)
    