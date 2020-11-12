import requests
import pytest
import random
from predictionserver.futureconventions.keyconventions import KeyConventions
from predictionserver.set_config import TESTING_KEYS
from predictionserver.clientmixins.basereader import BaseReader


@pytest.mark.usefixtures
def test_attribute_client_no_parser(attribute_client):
    """
      WHEN: need to use fixture providing localhost Flask app
      CHECK: we can call the local server
    """
    res = requests.patch(url='http://127.0.0.1:5000/OwnerPrivateAttribute')
    assert res.status_code==200


@pytest.mark.usefixtures
def test_attribute_client_with_parsers(attribute_client):
    """
      WHEN: need to use fixture providing localhost Flask app
      CHECK: its alive
    """
    write_key = random.choice(TESTING_KEYS)
    assert KeyConventions.is_valid_key(write_key=write_key)
    assert KeyConventions.key_difficulty(write_key=write_key)>=12
    payload = {'attribute_type': 'email', 'write_key': write_key}

    res1 = requests.patch(url='http://127.0.0.1:5000/OwnerPrivateAttribute')
    assert res1.status_code == 200
    res2 = requests.options(url='http://127.0.0.1:5000/OwnerPrivateAttribute')
    d = res2.json()
    pass


