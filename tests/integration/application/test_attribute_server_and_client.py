from predictionserver.clientmixins.attributereader import (
    AttributeReader, AttributeType, AttributeGranularity
)
from predictionserver.set_config import MICRO_TEST_CONFIG
from predictionserver.servermixins.attributeserver import AttributeServer

BABLOH_CATTLE = MICRO_TEST_CONFIG['BABLOH_CATTLE']

PUBLIC_PROFILE = {
    AttributeType.homepage: 'https://www.savetrumble.com.au',
    AttributeType.repository: 'https://pypi.org/project/microfilter/',
    AttributeType.paper: 'https://arxiv.org/pdf/1512.01389.pdf',
    AttributeType.topic: 'AutoMl',
    AttributeType.description: 'Herding cattle using AutoMl'
}

PRIVATE_PROFILE = {
    AttributeType.email: 'info@savetrundle.nsw.com.au',
    AttributeType.description: 'private description'
}


def test_attribute_server(localhost_process):
    """ Test using local flask app (see testconf.py """
    print('Running localhost_process test')
    ar = AttributeReader()
    ar.base_url = 'http://127.0.0.1:5000'
    server = AttributeServer()
    server.connect(**MICRO_TEST_CONFIG)
    email = 'babloh@cattle.com'
    server.set_attribute(
        attribute_type=AttributeType.email,
        granularity=AttributeGranularity.write_key,
        write_key=BABLOH_CATTLE,
        value=email
    )
    email_back = server.get_attribute(
        attribute_type=AttributeType.email,
        granularity=AttributeGranularity.write_key,
        write_key=BABLOH_CATTLE
    )
    assert email == email_back
    email_back_from_client = ar.get_attribute(
        attribute_type=AttributeType.email,
        granularity=AttributeGranularity.write_key,
        write_key=BABLOH_CATTLE
    )
    assert email == email_back_from_client
