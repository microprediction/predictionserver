from predictionserver.serverhabits.obscurityhabits import ObscurityHabits


class BalanceHabits(ObscurityHabits):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _BALANCES(self):
        return self.obscurity() + "balances"  # Hash of all balances attributed to write_keys

    def _RESERVE(self):
        return self.obscurity() + "reserve"
