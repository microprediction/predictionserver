from predictionserver.api.attributeapi import OwnerPublicAttributeApi, OwnerPrivateAttributeApi, StreamAttributeApi
import pytest
from predictionserver.set_config import MICRO_TEST_CONFIG
from predictionserver.futureconventions.memoconventions import MemoCategory, MemoGranularity
from predictionserver.futureconventions.activityconventions import Activity, ActivityContext
from predictionserver.futureconventions.attributeconventions import AttributeType
BABLOH_CATTLE = MICRO_TEST_CONFIG['BABLOH_CATTLE']


def test_init():
    ah = OwnerPublicAttributeApi()
    with pytest.raises(TypeError):
        ah = OwnerPublicAttributeApi(dog=6)


def test_public_attribute():
    api = OwnerPublicAttributeApi()
    api.connect(**MICRO_TEST_CONFIG)
    LI = 'https://my-li'
    attribute_type = str(AttributeType.linkedin)
    res = api.api_owner_public_attribute_put(attribute_type=attribute_type,
                                             value=LI, write_key=BABLOH_CATTLE)
    assert res['success'] == 1
    assert res['context'] == str(ActivityContext.attribute)

    value = api.api_owner_public_attribute_get(attribute_type=attribute_type,
                                               code=api.shash(BABLOH_CATTLE))
    assert value == LI
    # Test that memos are created
    user_memos = api.get_memos(category=MemoCategory.confirm, granularity=MemoGranularity.write_key,
                               write_key=BABLOH_CATTLE)
    assert len(user_memos) > 0
    memo = user_memos[0]
    assert memo['activity'] == str(Activity.set)
    assert memo['context'] == str(ActivityContext.attribute)
    assert memo['attribute'] == str(attribute_type)
    assert memo['success'] == 1
    assert memo['allowed'] == 1
    # Delete it
    memo = api.api_owner_public_attribute_delete(
        attribute_type=attribute_type, write_key=BABLOH_CATTLE)
    assert memo['activity'] == str(Activity.delete)
    assert memo['context'] == str(ActivityContext.attribute)
    assert memo['attribute'] == str(attribute_type)
    assert memo['success'] == 1
    assert memo['allowed'] == 1
    # Overwrite it
    res = api.api_owner_public_attribute_put(attribute_type=attribute_type,
                                             value='different@different.com', write_key=BABLOH_CATTLE)
    assert 'different@different.com' == api.api_owner_public_attribute_get(attribute_type=attribute_type,
                                                                           code=api.shash(BABLOH_CATTLE))


def test_stream_attribute():
    api = StreamAttributeApi()
    api.connect(**MICRO_TEST_CONFIG)
    name = 'stream_owner_by_bab.json'
    value = 'electricity'
    attribute_type = 'topic'
    api._delete_ownership_implementation(name=name, write_key=BABLOH_CATTLE)
    res = api.api_stream_attribute_put(attribute_type=attribute_type,
                                       name=name, write_key=BABLOH_CATTLE, value=value)
    assert res['success'] == 0  # Don't own it yet

    api._set_ownership_implementation(name=name, write_key=BABLOH_CATTLE)
    res2 = api.api_stream_attribute_put(attribute_type=attribute_type,
                                        name=name, write_key=BABLOH_CATTLE, value=value)
    assert res2['success'] == 1
    assert res2['context'] == 'attribute'
    assert res2['epoch_time'] > 10
    assert res2['success'] == 1

    # Now we own it
    res = api.api_stream_attribute_put(attribute_type=attribute_type, name=name,
                                       value=value, write_key=BABLOH_CATTLE)
    assert res['success'] == 1

    value_back = api.api_stream_attribute_get(attribute_type=attribute_type, name=name)
    assert value == value_back

    api.api_stream_attribute_delete(
        attribute_type=attribute_type, name=name, write_key=BABLOH_CATTLE)
    value_back = api.api_stream_attribute_get(attribute_type=attribute_type, name=name)
    assert value_back is None

    api._delete_ownership_implementation(name=name, write_key=BABLOH_CATTLE)


def test_private_attribute():
    api = OwnerPrivateAttributeApi()
    api.connect(**MICRO_TEST_CONFIG)
    EMAIL = 'my_private_email'
    attribute_type = str(AttributeType.email)
    res = api.api_owner_private_attribute_put(attribute_type=attribute_type,
                                              value=EMAIL, write_key=BABLOH_CATTLE)
    assert res['success'] == 1
    assert res['context'] == str(ActivityContext.attribute)

    value = api.api_owner_private_attribute_get(attribute_type=attribute_type,
                                                write_key=BABLOH_CATTLE)
    assert value == EMAIL
    # Test that memos are created
    user_memos = api.get_memos(category=MemoCategory.confirm, granularity=MemoGranularity.write_key,
                               write_key=BABLOH_CATTLE)
    assert len(user_memos) > 0
    memo = user_memos[0]
    assert memo['activity'] == str(Activity.set)
    assert memo['context'] == str(ActivityContext.attribute)
    assert memo['attribute'] == str(attribute_type)
    assert memo['success'] == 1
    assert memo['allowed'] == 1
    # Delete it
    memo = api.api_owner_private_attribute_delete(
        attribute_type=attribute_type, write_key=BABLOH_CATTLE)
    assert memo['activity'] == str(Activity.delete)
    assert memo['context'] == str(ActivityContext.attribute)
    assert memo['attribute'] == str(attribute_type)
    assert memo['success'] == 1
    assert memo['allowed'] == 1
    # Overwrite it
    res = api.api_owner_private_attribute_put(attribute_type=attribute_type,
                                              value='different@different.com', write_key=BABLOH_CATTLE)
    value_back = api.api_owner_private_attribute_get(
        attribute_type=attribute_type, write_key=BABLOH_CATTLE)
    assert 'different@different.com' == value_back
