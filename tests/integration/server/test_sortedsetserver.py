from predictionserver.servermixins.sortedsetserver import (
    SortedSetServer,
    SortedSetNameGranularity,
    SortedSetKeyGranularity,
    SortedSetType
)
from predictionserver.futureconventions.typeconventions import Genus, Memory
from predictionserver.set_config import MICRO_TEST_CONFIG
import pytest


BABLOH_CATTLE = MICRO_TEST_CONFIG['BABLOH_CATTLE']
PERFORMANCE = {BABLOH_CATTLE: 17.0}
LINKS = {'stream1.json': 0.5, 'stream2.json': 0.1}
HORIZON_PRIORITY = {
    'mystream3.json|310': 2 * 310,
    'mystream5.json|910': 2 * 910
}
STREAM_PRIORITY = {'mystream1': 1, 'mystream2': 2}


def test_init():
    server = SortedSetServer()
    with pytest.raises(RuntimeError):
        server.obscurity()
    server.connect(**MICRO_TEST_CONFIG)
    server.obscurity()


def test_leaderboard_not_allowed():
    server = SortedSetServer()
    server.connect(**MICRO_TEST_CONFIG)
    name = 'bananarama_sortedset.json'
    delay = 150
    code1 = server.shash(server.create_key(difficulty=6))
    name_kwargs = {
        'write_key': BABLOH_CATTLE,
        'name': name,
        'delay': delay,
        'genus': Genus.bivariate,
        'memory': Memory.short,
        'code': code1,
        'index': 17
    }
    write_key = BABLOH_CATTLE
    value = 31
    key_kwargs = {'code': code1}
    sortedset_type = SortedSetType.leaderboard
    name_granularity = SortedSetNameGranularity.name_and_delay
    key_granularity = SortedSetKeyGranularity.code
    res = server.add_sortedset_value(
        sortedset_type=sortedset_type,
        name_granularity=name_granularity,
        key_granularity=key_granularity,
        name_kwargs=name_kwargs,
        key_kwargs=key_kwargs,
        write_key=write_key,
        value=value, verbose=True
    )
    assert res['activity'] == 'add'
    assert res['sortedset_type'] == 'leaderboard'
    assert res['write_key'] == BABLOH_CATTLE
    assert res['allowed'] == 0
    assert res['success'] == 0

    pass
    # Again, less verbose
    assert not server.add_sortedset_value(
        sortedset_type=sortedset_type,
        name_granularity=name_granularity,
        key_granularity=key_granularity,
        name_kwargs=name_kwargs,
        key_kwargs=key_kwargs,
        write_key=write_key,
        value=value, verbose=False
    )


def test_leaderboard_allowed():
    server = SortedSetServer()
    server.connect(**MICRO_TEST_CONFIG)
    name = 'banana_rama_sortedset.json'
    server._set_ownership_implementation(name=name, write_key=BABLOH_CATTLE)
    delay = 150
    code1 = server.shash(server.create_key(difficulty=6))
    name_kwargs = {
        'write_key': BABLOH_CATTLE,
        'name': name,
        'delay': delay,
        'genus': Genus.bivariate,
        'memory': Memory.short,
        'code': code1,
        'index': 17
    }
    write_key = BABLOH_CATTLE
    value = 31
    key_kwargs = {'code': code1}
    sortedset_type = SortedSetType.leaderboard
    name_granularity = SortedSetNameGranularity.name_and_delay
    key_granularity = SortedSetKeyGranularity.code
    res = server.add_sortedset_value(
        sortedset_type=sortedset_type,
        name_granularity=name_granularity,
        key_granularity=key_granularity,
        name_kwargs=name_kwargs,
        key_kwargs=key_kwargs,
        write_key=write_key,
        value=value, verbose=True
    )
    assert res['activity'] == 'add'
    assert res['sortedset_type'] == 'leaderboard'
    assert res['write_key'] == BABLOH_CATTLE
    assert res['allowed'] == 1
    assert res['success'] == 1

    pass
    # Again, less verbose
    assert server.add_sortedset_value(
        sortedset_type=sortedset_type,
        name_granularity=name_granularity,
        key_granularity=key_granularity,
        name_kwargs=name_kwargs,
        key_kwargs=key_kwargs,
        write_key=write_key,
        value=value, verbose=False
    )

    values = server.get_sortedset_values(
        sortedset_type=sortedset_type,
        name_granularity=name_granularity,
        **name_kwargs
    )
    key = key_granularity.instance_name(**key_kwargs)
    assert values[key] == value

    # Scale it
    res = server.multiply_sortedset(
        sortedset_type=sortedset_type,
        name_granularity=name_granularity,
        name_kwargs=name_kwargs,
        write_key=write_key,
        verbose=True,
        weight=0.5
    )
    assert res['success'] == 1
    assert res['success'] == 1
    scaled_values = server.get_sortedset_values(
        sortedset_type=sortedset_type,
        name_granularity=name_granularity,
        **name_kwargs
    )
    for k, v in scaled_values.items():
        assert abs(v - 0.5 * values[k]) < 1e-6

    # Cause error when multiplying
    res = server.multiply_sortedset(
        sortedset_type=sortedset_type,
        name_granularity=name_granularity,
        name_kwargs=name_kwargs,
        write_key='wrong key',
        verbose=True,
        weight=0.5
    )
    assert res['execution'] == 0
    assert res['success'] == 0

    # Delete it all
    server.delete_sortedset(
        sortedset_type=sortedset_type,
        name_granularity=name_granularity,
        name_kwargs=name_kwargs,
        write_key=write_key,
        verbose=True
    )

    pass
    # Put it back incorrectly with invalid key
    invalid_res = server.add_sortedset_value(
        sortedset_type=sortedset_type,
        name_granularity=name_granularity,
        key_granularity=key_granularity,
        write_key='invalid key',
        name_kwargs=name_kwargs,
        key_kwargs=key_kwargs,
        value=value,
        verbose=True
    )
    assert invalid_res['allowed'] == 0
    assert res['success'] == 0

    # Put it back correctly
    assert server.add_sortedset_value(
        sortedset_type=sortedset_type,
        name_granularity=name_granularity,
        key_granularity=key_granularity,
        name_kwargs=name_kwargs,
        key_kwargs=key_kwargs,
        value=value,
        verbose=False
    )

    # Error deleting
    del_res_error = server.delete_sortedset(
        sortedset_type=sortedset_type,
        name_granularity=name_granularity,
        name_kwargs=name_kwargs,
        write_key='wrong_key',
        verbose=True
    )
    assert del_res_error['execution'] == 0
    assert del_res_error['success'] == 0

    values = server.get_sortedset_values(
        sortedset_type=sortedset_type,
        name_granularity=name_granularity,
        **name_kwargs
    )
    assert len(values) == 1

    server._delete_ownership_implementation(name=name, write_key=BABLOH_CATTLE)


def test_authorize():
    server = SortedSetServer()
    server.connect(**MICRO_TEST_CONFIG)
    # import uuid
    # name = str(uuid.uuid4()) + '.json'
    kwargs = {
        'name': 'my_stream_3.json',
        'delay': 310,
        'write_key': BABLOH_CATTLE
    }
    # Don't own it yet
    server._delete_ownership_implementation(name=kwargs['name'], write_key=BABLOH_CATTLE)
    res2 = server._authorize_sortedset_change(
        sortedset_type=SortedSetType.leaderboard,
        name_granularity=SortedSetNameGranularity.name_and_delay,
        **kwargs
    )
    assert not res2
    # Now we do
    server._set_ownership_implementation(name=kwargs['name'], write_key=BABLOH_CATTLE)
    assert server._authorize_sortedset_change(
        sortedset_type=SortedSetType.leaderboard,
        name_granularity=SortedSetNameGranularity.name_and_delay,
        **kwargs
    )
    server._delete_ownership_implementation(name=kwargs['name'], write_key=BABLOH_CATTLE)
