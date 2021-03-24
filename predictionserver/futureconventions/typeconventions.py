from typing import List, Union, Optional
from enum import Enum
from predictionserver.futureconventions.sepconventions import SepConventions

KeyList = List[Optional[str]]
NameList = List[Optional[str]]
Value = Union[str, int]
ValueList = List[Optional[Value]]
DelayList = List[Optional[int]]


class StrEnum(Enum):

    # Enum that prints itself with brevity

    def __str__(self):
        return super().__str__().split('.')[1]


class GranularityEnum(StrEnum):

    # Enum that assigns an instance name such as  'cop.json|310'

    def split(self):
        return self.__str__().split('_and_')

    def instance_name(self, **kwargs):
        non_none_kwargs = dict([(k, v) for k, v in kwargs.items() if v is not None])
        try:
            return SepConventions.pipe().join(
                [str(non_none_kwargs[k]) for k in self.split()])
        except KeyError:
            raise TypeError(
                'Cannot render instance name for granularity (missing kwarg like '
                'code, write_key, name etc)'
            )


class Memory(StrEnum):
    short = 0
    medium = 1
    long = 2


class Genus(StrEnum):
    regular = 0
    zscore = 1
    bivariate = 2
    trivariate = 3

    @staticmethod
    def from_name(name):
        return Genus.trivariate if 'z3~' in name else \
            Genus.bivariate if 'z2~' in name else \
            Genus.zscore if 'z1~' in name else \
            Genus.regular


class Family(StrEnum):
    name = 0
    delay = 1
    genus = 2
    sponsor = 3
