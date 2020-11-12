from pprint import pprint
from collections import OrderedDict
from predictionserver.futureconventions.horizonconventions import HorizonConventions
from predictionserver.futureconventions.typeconventions import GranularityEnum, StrEnum, Genus
from typing import Union
from predictionserver.futureconventions.keyconventions import KeyConventions
from logging import warning
from predictionserver.futureconventions.sepconventions import SepConventions


# There are different types of leaderboard. Each named
# leaderboard is represented internally as a sorted set.
#
# All leaderboards are keyed by code (i.e. public identity)
# All leaderboards are incremented by the system based when data arrives
# Some leaderboards aggregate performance across horizons, for a given sponsor (i.e. stream owner)
# These leaderboards can be reset by the stream owner, only.
#
# Leaderboards stochastically shrink. This means that every now and then, all values in the
# leaderboard will be multiplied by 0.9 (say). The frequency at which this shrinking occurs is
# determined by the medium term memory parameter in self.LEADERBOARD_MEMORIES (except for
# the special case where leaderboard granularity is specified as 'memory', in which case it
# might occur more or less often)


class LeaderboardGranularity(GranularityEnum):
    """ Enumerates granularity at which leaderboards are aggregated """
    memory = 0
    delay = 1
    name = 2
    code = 3                # public identity of stream owner
    genus = 4
    name_and_delay = 21
    code_and_delay = 31
    code_and_genus = 34


class LeaderboardMemoryDescription(StrEnum):
    short = 0
    medium = 1
    long = 2


class LeaderboardConventions:

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.LEADERBOARD = "leaderboard" + SepConventions.sep()
        self.CUSTOM_LEADERBOARD = 'custom_leaderboard' + SepConventions.sep()  # Deprecated
        self.LEADERBOARD_MEMORIES = OrderedDict({LeaderboardMemoryDescription.short:50,
                                                 LeaderboardMemoryDescription.medium:500,
                                                 LeaderboardMemoryDescription.long:5000})
                              # Memories tell the system how often to shrink all scores (Poisson process)
                              # If the LEADERBOARD_MULTIPLIER is close to 1.0 the effective memory will be longer then
                              # these numbers would indicate.
        self.LEADERBOARD_CAST = {'memory':int,'delay':int,'name':str,'code':str,'genus':str}
                              # Used when converting a leaderboard specification to dict
        self.LEADERBOARD_MULTIPLIER = 0.95
                              # How scores are shrunk towards zero

    def leaderboard_name_strict(self, granularity:LeaderboardGranularity,
                                memory:int=None,
                                name:str=None,
                                delay:int=None,
                                genus:Genus=None,
                                code:str=None):
        """ Leaderboard name """
        instance = granularity.instance_name(memory=memory, name=name, genus=genus, code=code, delay=delay)
        return self.LEADERBOARD + str(granularity) + SepConventions.sep() + instance

    def leaderboard_name(self, granularity:Union[LeaderboardGranularity, str],
                         memory_description:Union[LeaderboardMemoryDescription, str]=None,
                         memory:int=None,
                         delay:Union[int,str]=None,
                         name:str=None,
                         genus:Union[Genus,str]=None,
                         code:str=None):
        """ A more forgiving way to specify a leaderboard name, using str arguments rather than enum, and
            optionally using a memory descriptoin rather than numerical value """
        strict = self._leaderboard_strict_args(granularity=granularity, memory_description=memory_description, memory=memory,
                                               delay=delay, name=name, genus=genus, code=code)
        return self.leaderboard_name_strict(**strict)

    # ------------------------------------- #
    #   Some special cases of leaderboards  #
    # ------------------------------------- #

    def leaderboard_name_for_overall(self):
        """ The overall leaderboard is a synonym for memory aggregated leaderboard using medium term memory """
        return self.leaderboard_name(granularity=LeaderboardGranularity.memory,
                                     memory_description=LeaderboardMemoryDescription.medium)

    def leaderboard_name_for_movers(self):
        """ The movers leaderboard is a synonym for memory aggregated leaderboard using short term memory """
        return self.leaderboard_name(granularity=LeaderboardGranularity.memory,
                                     memory_description=LeaderboardMemoryDescription.short)

    def leaderboard_name_for_champions(self):
        """ The overall leaderboard is a synonym for memory aggregated leaderboard using long term memory """
        return self.leaderboard_name(granularity=LeaderboardGranularity.memory,
                                     memory_description=LeaderboardMemoryDescription.long)

    def leaderboard_name_for_stream(self, name: str) -> str:
        """ Name of overall medium term leaderboard associated with a stream """
        return self.leaderboard_name_strict(granularity=LeaderboardGranularity.name, name=name)

    def leaderboard_name_for_horizon(self, horizon: str = None, name: str = None, delay: str = None) -> str:
        """ Name of leaderboard associated with a stream and delay """
        if name is None:
            if delay is not None:
                warning('Possibly confusion in use of leaderboard_name_for_horizon. Both horizon and delay were specified')
            name, delay = HorizonConventions.split_horizon_name(horizon)
        return self.leaderboard_name_strict(granularity=LeaderboardGranularity.name_and_delay, name=name, delay=delay)

    def _leaderboard_sponsor_granularity_inference(self, delay=None, genus=None):
        """ Infer granularity from arguments supplied """
        if delay is None and genus is None:
            return LeaderboardGranularity.code
        elif delay is not None and genus is None:
            return LeaderboardGranularity.code_and_delay
        elif delay is None and genus is not None:
            return LeaderboardGranularity.code_and_genus
        else:
            raise Exception('Could not infer sponsor leaderboard granularity')

    def leaderboard_name_for_sponsor(self, code: str,
                                           genus: Union[str, Genus] = None,
                                           delay: int = None,
                                           granularity:LeaderboardGranularity=None) -> str:
        """ Name of sponsored leaderboard """
        # Good practice to provide the granularity explicitly, but it can be inferred
        if granularity is None:
            granularity = self._leaderboard_sponsor_granularity_inference(delay=delay, genus=genus)
        else:
            granularity = LeaderboardGranularity[str(granularity)]
        return self.leaderboard_name(granularity=granularity, code=code, genus=genus, delay=delay )


    # ------------------ #
    #   Implementation   #
    # ------------------ #


    def _leaderboard_strict_args(self, granularity: Union[LeaderboardGranularity, str],
                                 memory_description: Union[LeaderboardMemoryDescription, str] = None,
                                 memory: int = None,
                                 delay: Union[int, str] = None,
                                 genus: Union[Genus, str] = None,
                                 code: str = None,
                                 name: str = None):
        """ Convert from loose specification of leaderboard arguments to strict """
        if genus is not None:
            genus = Genus[str(genus)]
        if memory_description is not None:
            memory_description = LeaderboardMemoryDescription[str(memory_description)]
        if memory is None and memory_description is not None:
            memory = self.LEADERBOARD_MEMORIES[memory_description]
        if granularity is not None:
            granularity = LeaderboardGranularity[str(granularity)]
        if delay is not None:
            delay = int(delay)
            assert delay in self.DELAYS, 'Bad delay used in leaderboard context'
        if code is not None:
            code = KeyConventions.code_from_code_or_key(code_or_key=code)
        return {'genus': genus,
                'memory': memory,
                'granularity': granularity,
                'code': code,
                'name': name,
                'delay': delay}

    def leaderboard_name_as_dict(self, leaderboard_name):
        """ leaderboard::name_and_delay::john.json|310  ->  {name:john.json,delay:310}"""
        _, str_granularity, str_values = leaderboard_name.split(SepConventions.sep())
        things = str_granularity.split('_and_')
        values = str_values.split(SepConventions.pipe())
        return dict([ (k,self.LEADERBOARD_CAST[k](v)) for k,v in zip(things, values)])

    def leaderboard_memory_from_name(self,leaderboard_name:str) -> int:
        d = self.leaderboard_name_as_dict(leaderboard_name=leaderboard_name)
        return int(d[str(LeaderboardGranularity.memory)]) if str(LeaderboardGranularity.memory) in d else self.LEADERBOARD_MEMORIES['default']



