from predictionserver.serverhabits.attributehabits import (
    AttributeType, AttributeHabits, AttributeGranularity
)
from predictionserver.clientmixins.attributereader import AttributeReader
from predictionserver.futureconventions.typeconventions import Genus
from predictionserver.futureconventions.keyconventions import KeyConventions
import pytest
from predictionserver.set_config import MICRO_TEST_CONFIG


BABLOH_CATTLE = MICRO_TEST_CONFIG['BABLOH_CATTLE']


def test_init():
    AttributeReader()
    with pytest.raises(TypeError):
        AttributeHabits(dog=6)


def test_attribute_location():
    ah = AttributeHabits()
    loc = ah.attribute_location(
        attribute=AttributeType.article,
        granularity=AttributeGranularity.name_and_delay
    )
    assert loc == 'attributes::article::name_and_delay'
    for attribute in AttributeType:
        for granularity in AttributeGranularity:
            l1 = ah.attribute_location(attribute=attribute, granularity=granularity)
            l2 = ah.attribute_location(attribute=str(attribute), granularity=granularity)
            l3 = ah.attribute_location(attribute=attribute, granularity=str(granularity))
            l4 = ah.attribute_location(
                attribute=str(attribute), granularity=str(granularity)
            )
            assert l1 == 'attributes::' + str(attribute) + '::' + str(granularity)
            assert l2 == l1
            assert l3 == l1
            assert l4 == l1


def test_attribute_key():
    ah = AttributeHabits()
    assert ah.attribute_key(
        granularity=AttributeGranularity.name_and_delay,
        name='john.json',
        delay=17
    ) == 'john.json|17'
    with pytest.raises(TypeError):
        ah.attribute_key(granularity=AttributeGranularity.name_and_delay, name='mary')
    assert ah.attribute_key(
        name='johns_stream.json', delay=310
    ) == 'johns_stream.json|310'  # inference
    with pytest.raises(TypeError):
        ah.attribute_key(genus=Genus.regular)
    with pytest.raises(TypeError):
        ah.attribute_key(genus=Genus.bivariate, code='afakecode')
    real_code = KeyConventions.shash(BABLOH_CATTLE)
    assert ah.attribute_key(
        genus=Genus.bivariate, code=real_code
    ) == real_code + '|bivariate'
    assert ah.attribute_key(
        genus=Genus.trivariate, code=real_code
    ) == real_code + '|trivariate'
    assert ah.attribute_key(
        granularity=AttributeGranularity.code,
        write_key=BABLOH_CATTLE
    ) == real_code
    assert ah.attribute_key(
        granularity=AttributeGranularity.code,
        code=real_code
    ) == real_code
    with pytest.raises(TypeError):
        assert ah.attribute_key(
            granularity=AttributeGranularity.code,
            genus=Genus.bivariate
        ) == real_code


def test_attribute_location_by_inference():
    ah = AttributeHabits()
    assert ah.attribute_location_by_inference(
        attribute=AttributeType.article, name='mystream'
    ) == 'attributes::article::name'
    with pytest.raises(TypeError):
        ah.attribute_location_by_inference(
            attribute=AttributeType.article, genus=Genus.bivariate
        )


def test_attribute_location_and_key():
    ah = AttributeHabits()
    loc, key = ah.attribute_location_and_key(
        attribute=AttributeType.article,
        granularity=AttributeGranularity.name_and_delay,
        name='john.json',
        delay=17
    )
    assert key == 'john.json|17'
    assert loc == 'attributes::article::name_and_delay'
