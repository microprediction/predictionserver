from predictionserver.serverhabits.obscurityhabits import ObscurityHabits

# Ownership server has the limited responsibility of tracking the official
# owners of each stream
#
# Terminology:
#   - sponsor            refers to public identity (code, the shash of the write_key)
#   - owner/authority    refers to private identity, the write_key


class OwnershipHabits(ObscurityHabits):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _OWNERSHIP(self):
        return self.obscurity() + "ownership"   # Official map from name to write_key

    def _BLACKLIST(self):
        return self.obscurity() + "blacklist"   # List of discarded keys

    def _NAMES(self):
        # Location of redundant set of all stream names (needed for random
        # sampling when collecting garbage)
        return self.obscurity() + "names"
