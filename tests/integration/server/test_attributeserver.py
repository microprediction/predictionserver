from predictionserver.servermixins.attributeserver import AttributeServer, AttributeGranularity, AttributeType
from predictionserver.set_config import MICRO_TEST_CONFIG
from predictionserver.futureconventions.activityconventions import Activity, ActivityContext
from predictionserver.futureconventions.memoconventions import Memo, MemoCategory, MemoGranularity
import pytest

BABLOH_CATTLE = MICRO_TEST_CONFIG['BABLOH_CATTLE']

PUBLIC_PROFILE = {AttributeType.homepage: 'https://www.savetrumble.com.au',
                  AttributeType.repository: 'https://pypi.org/project/microfilter/',
                  AttributeType.paper: 'https://arxiv.org/pdf/1512.01389.pdf',
                  AttributeType.topic: 'AutoMl',
                  AttributeType.description: 'Hearding cattle using AutoMl'}

PRIVATE_PROFILE = {AttributeType.email: 'info@savetrundle.nsw.com.au',
                   AttributeType.description: 'private description'}


def test_init():
    server = AttributeServer()
    with pytest.raises(RuntimeError):
        server.obscurity()
    server.connect(**MICRO_TEST_CONFIG)
    obs = server.obscurity()


def test_set_private_attribute_explicit():
    server = AttributeServer()
    server.connect(**MICRO_TEST_CONFIG)
    EMAIL = 'me@private.com'
    assert server.set_attribute(attribute_type=AttributeType.email, granularity=AttributeGranularity.write_key,
                                value=EMAIL, write_key=BABLOH_CATTLE, verbose=False)
    value = server.get_attribute(attribute_type=AttributeType.email, granularity=AttributeGranularity.write_key,
                                 write_key=BABLOH_CATTLE)
    assert value == EMAIL
    # Test that memos are created
    user_memos = server.get_memos(category=MemoCategory.confirm, granularity=MemoGranularity.write_key,
                                  write_key=BABLOH_CATTLE)
    assert len(user_memos) > 0
    memo = user_memos[0]
    assert memo['activity'] == str(Activity.set)
    assert memo['context'] == str(ActivityContext.attribute)
    assert memo['attribute'] == str(AttributeType.email)
    assert memo['success'] == 1
    assert memo['allowed'] == 1


def test_set_private_attribute_explicit_verbose():
    server = AttributeServer()
    server.connect(**MICRO_TEST_CONFIG)
    EMAIL = 'me@private.com'
    memo = server.set_attribute(attribute_type=AttributeType.email, granularity=AttributeGranularity.write_key,
                                value=EMAIL, write_key=BABLOH_CATTLE, verbose=True)
    value = server.get_attribute(attribute_type=AttributeType.email, granularity=AttributeGranularity.write_key,
                                 write_key=BABLOH_CATTLE)
    assert value == EMAIL
    assert memo['activity'] == str(Activity.set)
    assert memo['context'] == str(ActivityContext.attribute)
    assert memo['success'] == 1
    assert memo['allowed'] == 1


def test_set_private_attribute_explicit_error_key():
    server = AttributeServer()
    server.connect(**MICRO_TEST_CONFIG)
    EMAIL = 'me@private.com'
    bad_key = 'lkasjdf'
    memo = server.set_attribute(attribute_type=AttributeType.email, granularity=AttributeGranularity.write_key,
                                value=EMAIL, write_key=bad_key, verbose=True)
    assert memo['activity'] == str(Activity.set)
    assert memo['context'] == str(ActivityContext.attribute)
    assert memo['success'] == 0
    assert memo['allowed'] == 0


def test_set_private_attribute_explicit_error_missing():
    server = AttributeServer()
    server.connect(**MICRO_TEST_CONFIG)
    EMAIL = 'me@private.com'
    memo = server.set_attribute(attribute_type=AttributeType.email, granularity=AttributeGranularity.name_and_delay,
                                value=EMAIL, write_key=BABLOH_CATTLE, verbose=True, name='bad name', delay=7)
    # Manually retrieve error
    user_errors = server.get_owner_errors(write_key=BABLOH_CATTLE)
    assert len(user_errors)
    memo = user_errors[0]
    assert memo['activity'] == str(Activity.set)
    assert memo['context'] == str(ActivityContext.attribute)
    assert memo['attribute'] == str(AttributeType.email)
    assert memo['success'] == 0
    assert memo['allowed'] == 0


def test_set_private_attribute_implicit():
    server = AttributeServer()
    server.connect(**MICRO_TEST_CONFIG)
    EMAIL = 'me@private.com'
    server.set_attribute(attribute_type=AttributeType.email, value=EMAIL, write_key=BABLOH_CATTLE)
    value = server.get_attribute(attribute_type=AttributeType.email, write_key=BABLOH_CATTLE)
    assert value == EMAIL
    value_again = server.get_owner_email(write_key=BABLOH_CATTLE)
    assert value_again == EMAIL


def test_set_code_attribute_implicit():
    server = AttributeServer()
    server.connect(**MICRO_TEST_CONFIG)
    code = server.shash(BABLOH_CATTLE)
    for attribute, value in PUBLIC_PROFILE.items():
        assert server.set_attribute(attribute_type=attribute, value=value, code=code, write_key=BABLOH_CATTLE,
                                    verbose=False)
    # Get back explicitly
    for attribute, value in PUBLIC_PROFILE.items():
        value_back = server.get_attribute(attribute_type=attribute, granularity=AttributeGranularity.code, code=code)
        assert value == value_back

    description = server.get_owner_description(code=code)
    assert description == PUBLIC_PROFILE[AttributeType.description]

    repo = server.get_owner_repository(code=code)
    assert repo == PUBLIC_PROFILE[AttributeType.repository]


def test_set_stream_attribute_bad_name():
    server = AttributeServer()
    server.connect(**MICRO_TEST_CONFIG)
    name = 'no_such_stream.json'
    value = 'Electricity'
    memo = server.set_attribute(attribute_type=AttributeType.description, granularity=AttributeGranularity.name,
                                value=value, name=name, verbose=True,
                                write_key=BABLOH_CATTLE)
    assert memo['allowed'] == False


def test_delete_stream_attribute_bad_name():
    server = AttributeServer()
    server.connect(**MICRO_TEST_CONFIG)
    name = 'no_such_stream.json'
    memo = server.delete_attribute(attribute_type=AttributeType.description, granularity=AttributeGranularity.name,
                                   name=name, verbose=True,
                                   write_key=BABLOH_CATTLE)
    assert memo['allowed'] == False


def test_delete_stream_attribute_manually():
    server = AttributeServer()
    server.connect(**MICRO_TEST_CONFIG)
    # Create fake ownership for testing
    # Should probably write a context manager for this as it can be dangerous if OWNERSHIP and NAMES gets out of whack
    name = 'another_stream.json'
    server.client.hset(name=server._OWNERSHIP(), key=name, value=BABLOH_CATTLE)
    try:
        value = 'Electricity'
        assert server.set_attribute(attribute_type=AttributeType.description, granularity=AttributeGranularity.name,
                                    value=value, name=name, verbose=False,
                                    write_key=BABLOH_CATTLE)
        description_back = server.get_attribute(attribute_type=AttributeType.description,
                                                granularity=AttributeGranularity.name,
                                                name=name, delay=server.DELAYS[0])
        assert description_back == value
        description_back_again = server.get_stream_description(name=name)
        assert description_back_again == value

        assert server.delete_attribute(attribute_type=AttributeType.description, granularity=AttributeGranularity.name,
                                       write_key=BABLOH_CATTLE, verbose=False,
                                       name=name, delay=server.DELAYS[0])
        description_back_again = server.get_stream_description(name=name)
        assert description_back_again is None

    except Exception as e:
        server.client.hdel(server._OWNERSHIP(), name)
        raise e

    server.client.hdel(server._OWNERSHIP(), name)
    description_back_again = server.get_attribute(attribute_type=AttributeType.description,
                                                  granularity=AttributeGranularity.name,
                                                  name=name, delay=server.DELAYS[0])
    assert description_back_again is None
    and_again = server.get_horizon_description(name=name, delay=server.DELAYS[0])
    assert and_again is None


def test_delete_horizon_attribute_manually():
    server = AttributeServer()
    server.connect(**MICRO_TEST_CONFIG)
    # Create fake ownership for testing
    # Should probably write a context manager for this as it can be dangerous if OWNERSHIP and NAMES gets out of whack
    name = 'fake_stream.json'
    server.client.hset(name=server._OWNERSHIP(), key=name, value=BABLOH_CATTLE)
    try:
        value = 'Electricity'
        assert server.set_attribute(attribute_type=AttributeType.description,
                                    granularity=AttributeGranularity.name_and_delay,
                                    value=value, name=name, delay=server.DELAYS[0], verbose=False,
                                    write_key=BABLOH_CATTLE)
        description_back = server.get_attribute(attribute_type=AttributeType.description,
                                                granularity=AttributeGranularity.name_and_delay,
                                                name=name, delay=server.DELAYS[0])
        assert description_back == value
        description_back_again = server.get_horizon_description(name=name, delay=server.DELAYS[0])
        assert description_back_again == value

        assert server.delete_attribute(attribute_type=AttributeType.description,
                                       granularity=AttributeGranularity.name_and_delay,
                                       write_key=BABLOH_CATTLE, verbose=False,
                                       name=name, delay=server.DELAYS[0])
    except Exception as e:
        server.client.hdel(server._OWNERSHIP(), name)
        raise e

    server.client.hdel(server._OWNERSHIP(), name)
    description_back_again = server.get_attribute(attribute_type=AttributeType.description,
                                                  granularity=AttributeGranularity.name_and_delay,
                                                  name=name, delay=server.DELAYS[0])
    assert description_back_again is None
    and_again = server.get_horizon_description(name=name, delay=server.DELAYS[0])
    assert and_again is None


def test_delete_horizon_attribute_manually_implicitly():
    server = AttributeServer()
    server.connect(**MICRO_TEST_CONFIG)
    # Create fake ownership for testing
    # Should probably write a context manager for this as it can be dangerous if OWNERSHIP and NAMES gets out of whack
    name = 'fake_stream.json'
    server.client.hset(name=server._OWNERSHIP(), key=name, value=BABLOH_CATTLE)
    try:
        value = 'Electricity'
        assert server.set_attribute(attribute_type=AttributeType.description,
                                    value=value, name=name, delay=server.DELAYS[0], verbose=False,
                                    write_key=BABLOH_CATTLE)
        description_back = server.get_attribute(attribute_type=AttributeType.description,
                                                granularity=AttributeGranularity.name_and_delay,
                                                name=name, delay=server.DELAYS[0])
        assert description_back == value
        description_back_again = server.get_horizon_description(name=name, delay=server.DELAYS[0])
        assert description_back_again == value

        assert server.delete_attribute(attribute_type=AttributeType.description,
                                       write_key=BABLOH_CATTLE, verbose=False,
                                       name=name, delay=server.DELAYS[0])


    except Exception as e:
        server.client.hdel(server._OWNERSHIP(), name)
        raise e

    server.client.hdel(server._OWNERSHIP(), name)
    description_back_again = server.get_attribute(attribute_type=AttributeType.description,
                                                  name=name, delay=server.DELAYS[0])
    assert description_back_again is None
    and_again = server.get_horizon_description(name=name, delay=server.DELAYS[0])
    assert and_again is None


# TODO: Test tripping of errors when attribute is not owned
# TODO: Test error memo generation
# TODO: Test confirm memo generation
# TODO: Test __delete_all_owner_attributes and ___delete_all_stream_attributes


STREAM_ATTRIBUTES = {AttributeType.description: 'COOL bananas',
                     AttributeType.homepage: 'www.mystream.com',
                     AttributeType.topic: 'Electricity',
                     AttributeType.article: 'www.linkedin.blah',
                     AttributeType.update: 'bankrupt'}


def test_delete_all_stream_attributes():
    server = AttributeServer()
    server.connect(**MICRO_TEST_CONFIG)
    # Create fake ownership for testing
    # Should probably write a context manager for this as it can be dangerous if OWNERSHIP and NAMES gets out of whack
    name = 'fake_stream.json'
    server.client.hset(name=server._OWNERSHIP(), key=name, value=BABLOH_CATTLE)

    try:
        for attribute, value in STREAM_ATTRIBUTES.items():
            assert server.set_attribute(attribute_type=AttributeType.description, granularity=AttributeGranularity.name,
                                        value=value, name=name, verbose=False, write_key=BABLOH_CATTLE)
            value_back = server.get_attribute(attribute_type=AttributeType.description,
                                              granularity=AttributeGranularity.name,
                                              name=name, delay=server.DELAYS[0])
            assert value_back == value

        pipe = server.client.pipeline()
        pipe = server._pipe_delete_all_stream_attributes(pipe=pipe, names=[name])
        execution = pipe.execute()
    except Exception as e:
        server.client.hdel(server._OWNERSHIP(), name)
        raise e

    server.client.hdel(server._OWNERSHIP(), name)
    for attribute, value in STREAM_ATTRIBUTES.items():
        value_back = server.get_attribute(attribute_type=attribute, name=name)
        assert value_back is None


def test_delete_all_public_owner_attributes():
    server = AttributeServer()
    server.connect(**MICRO_TEST_CONFIG)
    code = server.shash(BABLOH_CATTLE)
    for attribute, value in PUBLIC_PROFILE.items():
        assert server.set_attribute(attribute_type=attribute, value=value, code=code, write_key=BABLOH_CATTLE,
                                    verbose=False)

    for attribute, value in PUBLIC_PROFILE.items():
        value_back = server.get_attribute(attribute_type=attribute, code=code)
        assert value_back == value

    pipe = server.client.pipeline()
    pipe = server._pipe_delete_all_public_owner_attributes(pipe=pipe, write_key=BABLOH_CATTLE)
    execution = pipe.execute()
    assert sum(ex == 1 for ex in execution) >= len(PUBLIC_PROFILE)

    for attribute, value in PUBLIC_PROFILE.items():
        assert server.get_attribute(attribute_type=attribute, code=code) is None


def test_delete_all_private_owner_attributes():
    server = AttributeServer()
    server.connect(**MICRO_TEST_CONFIG)
    code = server.shash(BABLOH_CATTLE)
    for attribute, value in PRIVATE_PROFILE.items():
        assert server.set_attribute(attribute_type=attribute, value=value, write_key=BABLOH_CATTLE, verbose=False)

    for attribute, value in PRIVATE_PROFILE.items():
        value_back = server.get_attribute(attribute_type=attribute, write_key=BABLOH_CATTLE)
        assert value_back == value

    pipe = server.client.pipeline()
    pipe = server._pipe_delete_all_private_owner_attributes(pipe=pipe, write_key=BABLOH_CATTLE)
    execution = pipe.execute()
    assert sum(ex == 1 for ex in execution) >= len(PRIVATE_PROFILE)  # Other tests may confuse the issue

    for attribute, value in PRIVATE_PROFILE.items():
        assert server.get_attribute(attribute_type=attribute, code=code) is None
