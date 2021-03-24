from pprint import pprint
from predictionserver.futureconventions.keyconventions import KeyConventions
from predictionserver.futureconventions.activityconventions import Activity
import pytest
from predictionserver.set_config import MICRO_TEST_CONFIG
BABLOH_CATTLE = MICRO_TEST_CONFIG['BABLOH_CATTLE']


def test_init():
    kc = KeyConventions()
    assert kc.MIN_BALANCE == -1


def test_init_no_args():
    with pytest.raises(TypeError):
        kc = KeyConventions(dog=5)


def test_defaults():
    kc = KeyConventions()
    assert kc.MIN_DIFFICULTIES[Activity.set] > 10


def test_is_valid_key():
    kc = KeyConventions()
    assert kc.is_valid_key(write_key=BABLOH_CATTLE)
    assert not kc.is_valid_key(write_key='notakey')


def test_key_difficulty():
    assert KeyConventions.key_difficulty(BABLOH_CATTLE) == 12
    assert KeyConventions.key_difficulty('not a key') == 0


def test_create_key():
    new_key_6 = KeyConventions.create_key(difficulty=6, exact=False)
    new_key_6 = KeyConventions.create_key(difficulty=6, exact=True)
    assert KeyConventions.key_difficulty(write_key=new_key_6) == 6


def test_animal_from_key():
    code = KeyConventions.shash(BABLOH_CATTLE)
    animal = KeyConventions.animal_from_code(code=code)
    assert animal == 'Babloh Cattle'
    assert KeyConventions.animal_from_code('not a code') is None
    assert KeyConventions.animal_from_key(write_key=BABLOH_CATTLE) == 'Babloh Cattle'


def test_shash():
    assert len(KeyConventions.shash(write_key=BABLOH_CATTLE)) == len(BABLOH_CATTLE)
    assert len(KeyConventions.shash(write_key='DOG')) == len(BABLOH_CATTLE)


def test_maybe_create_key():
    new_key_6 = KeyConventions.maybe_create_key(difficulty=6)
    assert new_key_6 is None or KeyConventions.key_difficulty(write_key=new_key_6) >= 6


def test_code_from_code_or_key():
    assert KeyConventions.code_from_code_or_key(
        code_or_key=BABLOH_CATTLE) == KeyConventions.shash(write_key=BABLOH_CATTLE)
    code = KeyConventions.shash(BABLOH_CATTLE)
    assert KeyConventions.code_from_code_or_key(code_or_key=code) == code


def test_own_write_key():
    kc = KeyConventions()
    kc.write_key = BABLOH_CATTLE
    assert kc.own_write_key() == BABLOH_CATTLE
