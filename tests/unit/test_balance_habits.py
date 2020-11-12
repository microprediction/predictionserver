from predictionserver.serverhabits.balancehabits import BalanceHabits
import pytest
from predictionserver.futureconventions.keyconventions import KeyConventions

def test_init():
    bh = BalanceHabits()
    with pytest.raises(RuntimeError):
        bh._BALANCES()
    bh.set_obscurity(secret='secret sauce')
    assert bh._BALANCES() == 'secret sauce::balances'


def test_balances():
    bh = BalanceHabits()
    bh.set_obscurity(secret='secret sauce')
    assert bh._BALANCES() == 'secret sauce::balances'


def test_carryover_key():
    bh = BalanceHabits()
    bh.set_obscurity('really obscure')
    assert bh._RESERVE() == 'really obscure::reserve'


