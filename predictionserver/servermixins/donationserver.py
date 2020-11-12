from predictionserver.futureconventions.typeconventions import Activity
from predictionserver.servermixins.baseserver import BaseServer
from predictionserver.serverhabits.donationhabits import DonationHabits
from pprint import pprint


class DonationServer:

    def __init__(self,**kwargs):
        super().__init__(**kwargs)

    def get_donors(self):
        donors = self.client.hgetall(self._donors_name())
        return dict(sorted(donors.items(), key=lambda e: int(e[1]), reverse=True))

    def get_donations(self, len=None, with_key=False):
        if len is None:
            len = 12
        if with_key:
            return [(write_key, self.animal_from_key(write_key)) for write_key in
                    self.client.smembers(self._donation_name(len=len))]
        else:
            return [self.animal_from_key(write_key) for write_key in self.client.smembers(self._donation_name(len=len))]

    def put_donation(self, write_key, password, donor=None, verbose=True):
        self._put_donation_implementation(write_key=write_key,password=password,donor=donor,verbose=verbose)

    # --------------------------------------------------------------------------
    #            Donations of MUIDs
    # --------------------------------------------------------------------------

    def _put_donation_implementation(self, write_key, password, donor=None, verbose=True):
        official_password = self.__donation_password()
        donor = donor or 'anonymous'
        if password == official_password:
            len = self.key_difficulty(write_key)
            if self.permitted_to_set(write_key=write_key):
                if self.client.sadd(self._donation_name(len=len), write_key):
                    importance = 16 ** (len - self.MIN_DIFFICULTIES[Activity.set])
                    self.client.hincrby(name=self._donors_name(), key=donor.lower(), amount=importance)
                return {"operation": "donation", "success": True, "message": "Thanks, you can view it at",
                        "url": self.base_url + "donations/all".replace('//', '/')} if verbose else 1
            else:
                return {"operation": "donation", "success": False,
                        "message": "Invalid write key or not long enough"} if verbose else 0
        else:
            return {"operation": "donation", "success": False, "message": "Invalid password",
                    "hint": "Ends with " + str(official_password[-4:])} if verbose else 0


class StandaloneDonationServer(DonationServer,DonationHabits, BaseServer):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)


if __name__=="__main__":
    from predictionserver.collider_config_private import REDIZ_COLLIDER_CONFIG
    server = StandaloneDonationServer(**REDIZ_COLLIDER_CONFIG)
    pprint(server.get_donors())
