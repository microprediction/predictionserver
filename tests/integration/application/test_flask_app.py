import requests
import pytest
import random
from predictionserver.futureconventions.keyconventions import KeyConventions
from predictionserver.set_config import TESTING_KEYS
from predictionserver.clientmixins.basereader import BaseReader


@pytest.mark.usefixtures
def test_attribute_client_manual(attribute_client_manual):
    """
      WHEN: need to use fixture providing manual attribute client
      CHECK: we can call the server calls
    """
    write_key = random.choice(TESTING_KEYS)
    res = attribute_client_manual.put(
      f'/public_attribute/?attribute_type=repository&write_key={write_key}&value=google.com'
    )
    assert res.status_code == 200

    res = attribute_client_manual.get(
      f'/public_attribute/?attribute_type=repository&code={write_key}'
    )
    assert res.status_code == 200
    assert res.data == b"\"google.com\"\n"


@pytest.mark.usefixtures
def test_attribute_client_autogen(attribute_client_autogen):
    """
      WHEN: need to use fixture providing autogen attribute client
      CHECK: we can call the server calls
    """
    write_key = random.choice(TESTING_KEYS)
    assert KeyConventions.is_valid_key(write_key=write_key)
    assert KeyConventions.key_difficulty(write_key=write_key)>=12

    res = attribute_client_autogen.put(
      f'/OwnerPublicAttribute/?attribute_type=repository&write_key={write_key}&value=google.com'
    )
    assert res.status_code == 200

    res = attribute_client_autogen.get(
      f'/OwnerPublicAttribute/?attribute_type=repository&code={write_key}'
    )
    assert res.status_code == 200
    assert res.data == b"\"google.com\"\n"
