from predictionserver.futureconventions.hashconventions import (
    HashKeyGranularity, HashNameGranularity
)
import pytest


def test_type_error():
    with pytest.raises(AttributeError):
        HashNameGranularity.dog


def test_enum():
    assert HashNameGranularity[str(
        HashNameGranularity.write_key
    )] == HashNameGranularity.write_key
    assert HashNameGranularity[str(
        HashNameGranularity.name
    )] == HashNameGranularity.name
    assert HashKeyGranularity[str(
        HashKeyGranularity.name
    )] == HashKeyGranularity.name
    assert HashKeyGranularity[str(
        HashKeyGranularity.name_and_delay
    )] == HashKeyGranularity.name_and_delay
