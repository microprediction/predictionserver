from predictionserver.futureconventions.attributeconventions import (
    AttributeConventions,
    ATTRIBUTE_GRANULARITY_EXPLANATIONS,
    AttributeGranularity
)
from predictionserver.futureconventions.typeconventions import Genus
import pytest


def test_granularity_inference():
    ac = AttributeConventions()
    assert ac.attribute_granularity_inference(name='bill') == AttributeGranularity.name
    assert ac.attribute_granularity_inference(
        name='bill', delay=6) == AttributeGranularity.name_and_delay
    assert ac.attribute_granularity_inference(
        genus=Genus.bivariate, code='lakjsd') == AttributeGranularity.code_and_genus
    assert ac.attribute_granularity_inference(
        genus=Genus.trivariate, code='lakjsd') == AttributeGranularity.code_and_genus
    assert ac.attribute_granularity_inference(
        genus=Genus.trivariate, write_key='bernie') == AttributeGranularity.write_key
    assert ac.attribute_granularity_inference(
        genus=Genus.trivariate, code='bernie', write_key=None
    ) == AttributeGranularity.code_and_genus
    assert ac.attribute_granularity_inference(
        name='mary.json', delay=6, write_key='nowhere'
    ) == AttributeGranularity.name_and_delay
    with pytest.raises(TypeError):
        ac.attribute_granularity_inference()
    with pytest.raises(TypeError):
        ac.attribute_granularity_inference(genus='bill')
    assert ac.attribute_granularity_inference(code='nowhere') == AttributeGranularity.code


def test_enum():
    assert AttributeGranularity[str(
        AttributeGranularity.code_and_genus
    )] == AttributeGranularity.code_and_genus
    assert AttributeGranularity[str(
        AttributeGranularity.code_and_genus)] == AttributeGranularity.code_and_genus


def test_explanations():
    le = ATTRIBUTE_GRANULARITY_EXPLANATIONS
    assert len(ATTRIBUTE_GRANULARITY_EXPLANATIONS)
    assert isinstance(le[AttributeGranularity.write_key], str)
