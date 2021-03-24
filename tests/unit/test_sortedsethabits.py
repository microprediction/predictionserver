from predictionserver.serverhabits.sortedsethabits import SortedSetType, SortedSetNameGranularity, SortedSetKeyGranularity, SortedSetHabits
from predictionserver.futureconventions.typeconventions import Genus, Memory
import pytest
from predictionserver.set_config import MICRO_TEST_CONFIG
from predictionserver.futureconventions.keyconventions import KeyConventions

BABLOH_CATTLE = MICRO_TEST_CONFIG['BABLOH_CATTLE']


def test_init():
    ah = SortedSetHabits()
    with pytest.raises(TypeError):
        ah = SortedSetHabits(dog=6)


def test_sortedset_location():
    ah = SortedSetHabits()
    ah.set_obscurity(secret='boom')
    name = 'banana.json'
    delay = 310
    l = ah.sortedset_location(sortedset_type=SortedSetType.leaderboard, name_granularity=SortedSetNameGranularity.name_and_delay,
                              name=name, delay=delay)
    assert l == 'boom::sortedset::leaderboard::name_and_delay::'+name+'|'+str(delay)

    kwargs = {'write_key': BABLOH_CATTLE,
              'name': name,
              'delay': delay,
              'code': KeyConventions.shash(BABLOH_CATTLE),
              'genus': Genus.bivariate,
              'memory': Memory.short,
              'index': 31}
    for sortedset_type in SortedSetType:
        for name_granularity in SortedSetNameGranularity:
            l1 = ah.sortedset_location(sortedset_type=sortedset_type,
                                       name_granularity=name_granularity, **kwargs)
            l2 = ah.sortedset_location(sortedset_type=str(
                sortedset_type), name_granularity=name_granularity, **kwargs)
            l3 = ah.sortedset_location(sortedset_type=sortedset_type,
                                       name_granularity=str(name_granularity), **kwargs)
            l4 = ah.sortedset_location(sortedset_type=str(
                sortedset_type), name_granularity=str(name_granularity), **kwargs)
            assert l1 == 'boom::sortedset::' + \
                str(sortedset_type)+'::'+str(name_granularity) + \
                '::'+name_granularity.instance_name(**kwargs)
            assert l2 == l2
            assert l3 == l1
            assert l4 == l1


def test_sortedset_key():
    ah = SortedSetHabits()
    secret = 'boo'
    ah.set_obscurity(secret=secret)
    name = 'bananarama_sortedset.json'
    delay = 150
    kwargs = {'write_key': BABLOH_CATTLE,
              'name': name,
              'delay': delay,
              'genus': Genus.bivariate,
              'memory': Memory.short,
              'code': BABLOH_CATTLE,
              'index': 17,
              'other': 'side'}
    for key_granularity in SortedSetKeyGranularity:
        l1 = ah.sortedset_key(key_granularity=key_granularity, **kwargs)
        l2 = ah.sortedset_key(key_granularity=str(key_granularity), **kwargs)
        expected = key_granularity.instance_name(**kwargs)
        assert l1 == expected
        assert l2 == expected
