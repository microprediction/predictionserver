from pprint import pprint
from predictionserver.futureconventions.keyconventions import KeyConventions
from predictionserver.futureconventions.budgetconventions import BudgetConventions
from predictionserver.futureconventions.activityconventions import Activity
import pytest
from predictionserver.set_config import MICRO_TEST_CONFIG
BABLOH_CATTLE = MICRO_TEST_CONFIG['BABLOH_CATTLE']


def test_init():
    kc = BudgetConventions()
    assert kc.MIN_BALANCE == -1


def test_init_no_args():
    with pytest.raises(TypeError):
        kc = BudgetConventions(dog=5)


def test_defaults():
    kc = BudgetConventions()
    assert kc.MIN_DIFFICULTIES[Activity.set] > 10


class BalanceMocker(BudgetConventions, KeyConventions):

    def __init__(self):
        super().__init__()
        self.balance = 17

    def get_balance(self, write_key):
        return self.balance


def test_own_write_key():
    bc = BalanceMocker()
    bc.write_key = BABLOH_CATTLE
    assert bc.own_write_key() == BABLOH_CATTLE


def test_get_own_balance():
    mock = BalanceMocker()
    with pytest.raises(AttributeError):
        mock.get_own_balance()
    mock.write_key = BABLOH_CATTLE
    assert mock.get_own_balance() == 17


def test_bankrupt():
    mock = BalanceMocker()
    mock.write_key = BABLOH_CATTLE
    assert not mock.bankrupt()
    mock.balance = -10000
    assert mock.bankrupt()


def test_distance_to_bankruptcy():
    mock = BalanceMocker()
    with pytest.raises(AttributeError):
        mock.distance_to_bankruptcy()
    with pytest.raises(AttributeError):
        mock.distance_to_bankruptcy(level=-10)
    assert mock.distance_to_bankruptcy(balance=0, level=-10) == 10
    mock.write_key = BABLOH_CATTLE
    assert mock.distance_to_bankruptcy(balance=0) == 4096
    mock.balance = 4
    assert mock.distance_to_bankruptcy() == 4100
