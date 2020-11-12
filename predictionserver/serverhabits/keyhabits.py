from predictionserver.serverhabits.obscurityhabits import ObscurityHabits
from predictionserver.futureconventions.keyconventions import KeyConventions


class KeyHabits(KeyConventions,ObscurityHabits):

    def __init__(self,**kwargs):
        super().__init__(**kwargs)

