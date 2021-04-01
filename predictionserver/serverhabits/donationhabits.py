from predictionserver.serverhabits.obscurityhabits import ObscurityHabits
from predictionserver.futureconventions.keyconventions import KeyConventions
from microconventions.sep_conventions import SepConventions


class DonationHabits(ObscurityHabits):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._DONORS = "donors"

    def _DONATIONS(self):
        raise Exception("you want self.donation_location()")

    def _DONATION_PASSWORD(self):
        raise Exception("you want self.donation_password()")

    def __donation_password(self):
        return KeyConventions.shash(write_key=self.obscurity())

    def _donation_name(self, len):
        return self.donation_location() + str(len)

    def _donors_name(self):
        return self._DONORS

    def donation_location(self):
        return "donations" + SepConventions.sep()

    def donation_password(self):
        return KeyConventions.shash(self.obscurity())
