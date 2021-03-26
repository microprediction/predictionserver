from predictionserver.serverhabits.hashhabits import (
    HashType, HashNameGranularity, HashKeyGranularity, HashHabits
)
from predictionserver.futureconventions.keyconventions import KeyConventions
import pytest
from predictionserver.set_config import MICRO_TEST_CONFIG


BABLOH_CATTLE = MICRO_TEST_CONFIG['BABLOH_CATTLE']


def test_init():
    HashHabits()
    with pytest.raises(TypeError):
        _ = HashHabits(dog=6)


def test_hash_location():
    ah = HashHabits()
    ah.set_obscurity(secret='boom')
    name = 'banana.json'
    delay = 310
    loc = ah.hash_location(
        hash_type=HashType.performance,
        name_granularity=HashNameGranularity.write_key,
        write_key=BABLOH_CATTLE
    )
    assert loc == 'boom::hash::performance::write_key::' + BABLOH_CATTLE
    l1 = ah.hash_location(
        hash_type=HashType.priority,
        name_granularity=HashNameGranularity.name_and_delay,
        name=name,
        delay=delay
    )
    assert l1 == 'boom::hash::priority::name_and_delay::' + name + '|' + str(delay)

    kwargs = {
        'write_key': BABLOH_CATTLE,
        'name': name,
        'delay': delay,
        'other': 'side',
        'code': KeyConventions.shash(BABLOH_CATTLE)
    }
    for hash_type in HashType:
        for name_granularity in HashNameGranularity:
            l1 = ah.hash_location(
                hash_type=hash_type, name_granularity=name_granularity, **kwargs
            )
            l2 = ah.hash_location(
                hash_type=str(hash_type), name_granularity=name_granularity, **kwargs
            )
            l3 = ah.hash_location(
                hash_type=hash_type, name_granularity=str(name_granularity), **kwargs
            )
            l4 = ah.hash_location(
                hash_type=str(hash_type), name_granularity=str(name_granularity), **kwargs
            )
            assert l1 == 'boom::hash::' + str(hash_type) + '::' + str(name_granularity) + \
                   '::' + name_granularity.instance_name(**kwargs)
            assert l2 == l1
            assert l3 == l1
            assert l4 == l1


def test_hash_key():
    ah = HashHabits()
    secret = 'boo'
    ah.set_obscurity(secret=secret)
    name = 'bananarama'
    delay = 150
    kwargs = {
        'write_key': BABLOH_CATTLE,
        'name': name,
        'delay': delay,
        'other': 'side',
        'code': KeyConventions.shash(BABLOH_CATTLE)
    }

    for key_granularity in HashKeyGranularity:
        l1 = ah.hash_key(key_granularity=key_granularity, **kwargs)
        l2 = ah.hash_key(key_granularity=str(key_granularity), **kwargs)
        expected = key_granularity.instance_name(**kwargs)
        assert l1 == expected
        assert l2 == expected
