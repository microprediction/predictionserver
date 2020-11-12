from predictionserver.futureconventions.sortedsetconventions import SortedSetConventions,\
    SortedSetType, SortedSetNameGranularity, SortedSetKeyGranularity
import pytest


def test_type_error():
    with pytest.raises(AttributeError):
        SortedSetKeyGranularity.dog


def test_enum():
    assert SortedSetKeyGranularity[str(SortedSetKeyGranularity.code)] == SortedSetKeyGranularity.code
    assert SortedSetNameGranularity[str(SortedSetNameGranularity.name_and_delay)] == SortedSetNameGranularity.name_and_delay


def test_init():
    ssc = SortedSetConventions()






