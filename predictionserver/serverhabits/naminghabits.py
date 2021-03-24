from predictionserver.futureconventions.namingconventions import NamingConventions
from predictionserver.serverhabits.obscurityhabits import ObscurityHabits

# Random stuff here until it find a better place

BASE_SERVER_CONFIG_ARGS = ('history_len', 'delays', 'lagged_len', 'windows', 'obscurity')
DEFAULT_CONVENTIONS = {
    'min_len': 12,
    'min_balance': -1,
    'delays': [70, 310, 910, 3555],
    'num_predictions': 225
}
DEFAULT_TESTING_CONVENTIONS = {
    'min_len': 12,
    'min_balance': -1,
    'delays': [1, 5],
    'num_predictions': 225
}


class NamingHabits(NamingConventions, ObscurityHabits):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
