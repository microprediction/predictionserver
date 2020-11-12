from predictionserver.servermixins.hashserver import HashServer, HashNameGranularity, \
    HashKeyGranularity, HashType
from predictionserver.set_config import MICRO_TEST_CONFIG
import pytest

BABLOH_CATTLE = MICRO_TEST_CONFIG['BABLOH_CATTLE']
PERFORMANCE = {BABLOH_CATTLE: 17.0}
LINKS = {'stream1.json': 0.5,
         'stream2.json': 0.1}
HORIZON_PRIORITY = {'mystream3.json|310': 2 * 310,
                    'mystream5.json|910': 2 * 910}
STREAM_PRIORITY = {'mystream1': 1,
                   'mystream2': 2}


def test_init():
    server = HashServer()
    with pytest.raises(RuntimeError):
        server.obscurity()
    server.connect(**MICRO_TEST_CONFIG)
    obs = server.obscurity()


def test_private_hash():
    server = HashServer()
    server.connect(**MICRO_TEST_CONFIG)
    kwargs = {'name': 'my_private_stream.json',
              'delay': 310,
              'write_key': BABLOH_CATTLE}
    value = 31
    res = server.set_hash_value(hash_type=HashType.performance,
                                name_granularity=HashNameGranularity.write_key,
                                key_granularity=HashKeyGranularity.name_and_delay,
                                name_kwargs=kwargs, key_kwargs=kwargs,
                                value=value, verbose=True)
    assert res['activity'] == 'set'
    assert res['hash_type'] == 'performance'
    assert res['write_key'] == BABLOH_CATTLE
    assert res['delay'] == kwargs['delay']
    assert res['allowed'] == 1
    assert res['success'] == 1

    pass
    # Again, less verbose
    assert server.set_hash_value(hash_type=HashType.performance,
                                 name_granularity=HashNameGranularity.write_key,
                                 key_granularity=HashKeyGranularity.name_and_delay,
                                 name_kwargs=kwargs, key_kwargs=kwargs,
                                 value=value, verbose=False)

    value_back = server.get_hash_value(hash_type=HashType.performance,
                                       name_granularity=HashNameGranularity.write_key,
                                       key_granularity=HashKeyGranularity.name_and_delay,
                                       name_kwargs=kwargs, key_kwargs=kwargs)
    assert value == value_back
    values = server.get_hash_values(hash_type=HashType.performance,
                                    name_granularity=HashNameGranularity.write_key,
                                    **kwargs)
    key = HashKeyGranularity.name_and_delay.instance_name(**kwargs)
    assert values[key] == value

    # Scale it
    res = server.multiply_hash(hash_type=HashType.performance,
                               name_granularity=HashNameGranularity.write_key,
                               name_kwargs=kwargs, write_key=kwargs['write_key'],
                               verbose=True, weight=0.5)
    assert res['execution'] == 1
    assert res['success'] == 1
    scaled_values = server.get_hash_values(hash_type=HashType.performance,
                                           name_granularity=HashNameGranularity.write_key,
                                           **kwargs)
    for k, v in scaled_values.items():
        assert abs(v - 0.5 * values[k]) < 1e-6

    # Cause error when multiplying
    res = server.multiply_hash(hash_type=HashType.performance,
                               name_granularity=HashNameGranularity.write_key,
                               name_kwargs=kwargs, write_key='wrong key',
                               verbose=True, weight=0.5)
    assert res['execution'] == 0
    assert res['success'] == 0

    # Delete one entry
    del_val_res = server.delete_hash_value(hash_type=HashType.performance,
                                           name_granularity=HashNameGranularity.write_key,
                                           key_granularity=HashKeyGranularity.name_and_delay,
                                           name_kwargs=kwargs, write_key=kwargs['write_key'],
                                           key_kwargs=kwargs,
                                           verbose=True)
    assert del_val_res['hash_type'] == 'performance'
    value = server.get_hash_value(hash_type=HashType.performance,
                                  name_granularity=HashNameGranularity.write_key,
                                  key_granularity=HashKeyGranularity.name_and_delay,
                                  name_kwargs=kwargs,
                                  key_kwargs=kwargs)
    assert value == 0
    # Put it back
    assert server.set_hash_value(hash_type=HashType.performance,
                                 name_granularity=HashNameGranularity.write_key,
                                 key_granularity=HashKeyGranularity.name_and_delay,
                                 name_kwargs=kwargs, key_kwargs=kwargs,
                                 value=value, verbose=False)

    # Delete it all
    del_res = server.delete_hash(hash_type=HashType.performance,
                                 name_granularity=HashNameGranularity.write_key,
                                 name_kwargs=kwargs, write_key=kwargs['write_key'], verbose=True)

    pass
    # Put it back incorrectly with invalid key
    invalid_name_kwargs = {'write_key': 'wrong_key'}
    invalid_res = server.set_hash_value(hash_type=HashType.performance,
                                        name_granularity=HashNameGranularity.write_key,
                                        key_granularity=HashKeyGranularity.name_and_delay,
                                        name_kwargs=invalid_name_kwargs, key_kwargs=kwargs,
                                        value=value, verbose=True)
    assert invalid_res['allowed'] == 0
    assert res['success'] == 0

    # Put it back correctly
    assert server.set_hash_value(hash_type=HashType.performance,
                                 name_granularity=HashNameGranularity.write_key,
                                 key_granularity=HashKeyGranularity.name_and_delay,
                                 name_kwargs=kwargs, key_kwargs=kwargs,
                                 value=value, verbose=False)

    # Error deleting
    del_res_error = server.delete_hash(hash_type=HashType.performance,
                                       name_granularity=HashNameGranularity.write_key,
                                       name_kwargs=kwargs, write_key='wrong_key', verbose=True)
    assert del_res_error['execution'] == 0
    assert del_res_error['success'] == 0

    values = server.get_hash_values(hash_type=HashType.performance,
                                    name_granularity=HashNameGranularity.write_key,
                                    **kwargs)
    assert len(values) == 1


def test_public_hash():
    server = HashServer()
    server.connect(**MICRO_TEST_CONFIG)
    kwargs = {'name': 'my_other_other_stream.json',
              'delay': 310,
              'write_key': BABLOH_CATTLE,
              'code': server.shash(write_key=BABLOH_CATTLE)}
    value = 31
    res = server.set_hash_value(hash_type=HashType.performance,
                                name_granularity=HashNameGranularity.code,
                                key_granularity=HashKeyGranularity.name_and_delay,
                                name_kwargs=kwargs, key_kwargs=kwargs,
                                value=value, verbose=True)
    assert res['activity'] == 'set'
    assert res['hash_type'] == 'performance'
    assert res['write_key'] == BABLOH_CATTLE
    assert res['delay'] == kwargs['delay']
    assert res['allowed'] == 1
    assert res['success'] == 1
    pass
    # Again, less verbose
    assert server.set_hash_value(hash_type=HashType.performance,
                                 name_granularity=HashNameGranularity.code,
                                 key_granularity=HashKeyGranularity.name_and_delay,
                                 name_kwargs=kwargs, key_kwargs=kwargs,
                                 value=value, verbose=False)

    value_back = server.get_hash_value(hash_type=HashType.performance,
                                       name_granularity=HashNameGranularity.code,
                                       key_granularity=HashKeyGranularity.name_and_delay,
                                       name_kwargs=kwargs, key_kwargs=kwargs)
    assert value == value_back
    values = server.get_hash_values(hash_type=HashType.performance,
                                    name_granularity=HashNameGranularity.code,
                                    **kwargs)
    key = HashKeyGranularity.name_and_delay.instance_name(**kwargs)
    assert values[key] == value

    # Scale it
    res = server.multiply_hash(hash_type=HashType.performance,
                               name_granularity=HashNameGranularity.code,
                               name_kwargs=kwargs, write_key=kwargs['write_key'],
                               verbose=True, weight=0.5)
    assert res['execution'] == 1
    assert res['success'] == 1
    scaled_values = server.get_hash_values(hash_type=HashType.performance,
                                           name_granularity=HashNameGranularity.code,
                                           **kwargs)
    for k, v in scaled_values.items():
        assert abs(v - 0.5 * values[k]) < 1e-6

    # Cause error when multiplying due to wrong key
    res = server.multiply_hash(hash_type=HashType.performance,
                               name_granularity=HashNameGranularity.code,
                               name_kwargs=kwargs, write_key='wrong key',
                               verbose=True, weight=0.5)
    assert res['activity'] == 'multiply'
    assert res['name'] == kwargs['name']
    assert res['code'] == kwargs['code']
    # Delete it
    del_res = server.delete_hash(hash_type=HashType.performance,
                                 name_granularity=HashNameGranularity.code,
                                 name_kwargs=kwargs, write_key=kwargs['write_key'], verbose=True)

    assert del_res['activity'] == 'delete'
    assert del_res['hash_type'] == 'performance'
    values = server.get_hash_values(hash_type=HashType.performance,
                                    name_granularity=HashNameGranularity.code,
                                    **kwargs)
    assert len(values) == 0


def test_stream_horizon_hash():
    server = HashServer()
    server.connect(**MICRO_TEST_CONFIG)
    kwargs = {'name': 'my_different_stream.json',
              'delay': 310,
              'write_key': BABLOH_CATTLE,
              'code': server.shash(BABLOH_CATTLE)}
    value = 31
    for hash_type in HashType:
        name_granularity = HashNameGranularity.name_and_delay
        key_granularity = HashKeyGranularity.code
        server._set_ownership_implementation(name=kwargs['name'], write_key=kwargs['write_key'])
        res = server.set_hash_value(hash_type=hash_type,
                                    name_granularity=name_granularity,
                                    key_granularity=key_granularity,
                                    name_kwargs=kwargs, key_kwargs=kwargs,
                                    value=value, verbose=True)
        assert res['activity'] == 'set'
        assert res['hash_type'] == str(hash_type)
        assert res['write_key'] == BABLOH_CATTLE
        assert res['delay'] == kwargs['delay']
        assert res['allowed'] == 1
        assert res['success'] == 1
        # Again, less verbose
        assert server.set_hash_value(hash_type=hash_type,
                                     name_granularity=name_granularity,
                                     key_granularity=key_granularity,
                                     name_kwargs=kwargs, key_kwargs=kwargs,
                                     value=value, verbose=False)

        value_back = server.get_hash_value(hash_type=hash_type,
                                           name_granularity=name_granularity,
                                           key_granularity=key_granularity,
                                           name_kwargs=kwargs, key_kwargs=kwargs)
        assert value == value_back
        values = server.get_hash_values(hash_type=hash_type,
                                        name_granularity=name_granularity,
                                        **kwargs)
        key = key_granularity.instance_name(**kwargs)
        assert values[key] == value

        # Put it back incorrectly with valid key but not the right one for the stream
        valid_key = server.create_key(difficulty=7)
        from copy import deepcopy
        wrong_name_kwargs = deepcopy(kwargs)
        wrong_name_kwargs.update({'write_key': valid_key})
        wrong_res = server.set_hash_value(hash_type=hash_type,
                                          name_granularity=name_granularity,
                                          key_granularity=key_granularity,
                                          name_kwargs=wrong_name_kwargs, key_kwargs=kwargs,
                                          value=value, verbose=True)
        assert wrong_res['allowed'] == 0
        assert wrong_res['success'] == 0

        # Scale it
        res = server.multiply_hash(hash_type=hash_type,
                                   name_granularity=name_granularity,
                                   name_kwargs=kwargs, write_key=kwargs['write_key'],
                                   verbose=True, weight=0.5)
        assert res['execution'] == 1
        scaled_values = server.get_hash_values(hash_type=hash_type,
                                               name_granularity=name_granularity,
                                               **kwargs)
        for k, v in scaled_values.items():
            assert abs(v - 0.5 * values[k]) < 1e-6

        # Cause error when multiplying due to invalid key
        res = server.multiply_hash(hash_type=hash_type,
                                   name_granularity=name_granularity,
                                   name_kwargs=kwargs, write_key='wrong key',
                                   verbose=True, weight=0.5)
        assert res['activity'] == 'multiply'
        assert res['name'] == kwargs['name']
        assert res['code'] == kwargs['code']
        assert res['allowed'] == 0
        assert res['success'] == 0

        # Cause error when multiplying due to wrong key
        res = server.multiply_hash(hash_type=hash_type,
                                   name_granularity=name_granularity,
                                   name_kwargs=kwargs, write_key=valid_key,
                                   verbose=True, weight=0.5)
        assert res['activity'] == 'multiply'
        assert res['name'] == kwargs['name']
        assert res['code'] == kwargs['code']
        assert res['allowed'] == 0
        assert res['success'] == 0

        # Increment
        pipe = server.client.pipeline()
        pipe = server._pipe_incr_hash_value_implementation(pipe=pipe, hash_type=hash_type,
                                                           name_granularity=name_granularity,
                                                           key_granularity=key_granularity,
                                                           name_kwargs=kwargs,
                                                           key_kwargs=kwargs,
                                                           amount=1.0)
        pipe.execute()
        incremented_values = server.get_hash_values(hash_type=hash_type,
                                                    name_granularity=name_granularity,
                                                    **kwargs)
        for k, v in incremented_values.items():
            assert abs(v - (scaled_values[k]+1.0)) < 1e-6

        # Delete it
        del_res = server.delete_hash(hash_type=hash_type,
                                     name_granularity=name_granularity,
                                     name_kwargs=kwargs, write_key=kwargs['write_key'], verbose=True)

        assert del_res['success'] == 1
        assert del_res['success'] == 1

        values = server.get_hash_values(hash_type=hash_type,
                                        name_granularity=name_granularity,
                                        **kwargs)
        assert len(values) == 0

        server._delete_ownership_implementation(name=kwargs['name'], write_key=kwargs['write_key'])


def test_authorize():
    server = HashServer()
    server.connect(**MICRO_TEST_CONFIG)
    kwargs = {'name': 'my_silly_stream.json',
              'delay': 310,
              'write_key': BABLOH_CATTLE,
              'code': server.shash(BABLOH_CATTLE)}
    assert server._authorize_hash_change(hash_type=HashType.performance, name_granularity=HashNameGranularity.other)
