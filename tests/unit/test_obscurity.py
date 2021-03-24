
from predictionserver.serverhabits.obscurityhabits import ObscurityHabits
import pytest


def test_obscurity_init():
    obs = ObscurityHabits()
    with pytest.raises(Exception):
        obs.obscurity()
    obs.set_obscurity(secret='testing')
    assert obs.obscurity() == 'testing::'
