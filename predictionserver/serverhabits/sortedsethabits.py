from predictionserver.futureconventions.sortedsetconventions import SortedSetType, SortedSetConventions, SortedSetNameGranularity,SortedSetKeyGranularity
from predictionserver.futureconventions.sepconventions import SepConventions
from predictionserver.serverhabits.obscurityhabits import ObscurityHabits
from typing import Union


class SortedSetHabits(SortedSetConventions, ObscurityHabits):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._HASH = 'sortedset'

    def sortedset_location(self, sortedset_type: Union[SortedSetType, str], name_granularity: Union[SortedSetNameGranularity, str],**kwargs):
        """ Determine sortedset sortedset sortedset name, such as  sortedset::email::write_key    """
        sortedset_type = SortedSetType[str(sortedset_type)]
        name_granularity = SortedSetNameGranularity[str(name_granularity)]
        instance = name_granularity.instance_name(**kwargs)
        return self.obscurity() + SepConventions.sep().join([self._HASH, str(sortedset_type), str(name_granularity), instance])
      
    def sortedset_key(self, key_granularity:Union[SortedSetKeyGranularity,str], **kwargs):
        key_granularity = SortedSetKeyGranularity[str(key_granularity)]
        return key_granularity.instance_name(**kwargs)
    