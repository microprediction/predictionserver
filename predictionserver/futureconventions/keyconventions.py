import muid
import time
from muid.mining import mine_once
from predictionserver.futureconventions.activityconventions import Activity
import logging


class KeyConventions:

    """ Conventions for write_keys, which are Memorable Unique Identifiers (MUIDs)
        See www.muid.org or www.microprediction.com for more information
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.MIN_BALANCE = -1
        self.MIN_DIFFICULTIES = {
            Activity.set: 12,
            Activity.mset: 13,
            Activity.submit: 8,
            Activity.cset: 12
        }

    @staticmethod
    def is_valid_key(write_key):
        """ Check if key is hash-memorable """
        return isinstance(write_key, str) and muid.validate(write_key)

    # ------------------------------------------------------------------------------- #
    #   Creating keys, interpreting keys  (public and private identities)             #
    # ------------------------------------------------------------------------------- #

    @staticmethod
    def create_key(difficulty=8, exact=False):
        """ Create new write_key (string, not bytes)

                exact - Insist on supplied difficulty.
                        If exact is not set to True, the key might be higher difficulty
                        than requested
        """
        assert difficulty < 18, "Be realistic!"
        while True:
            write_key = muid.create(difficulty=difficulty).decode()
            if not exact:
                return write_key
            else:
                actual_difficulty = KeyConventions.key_difficulty(write_key=write_key)
                if difficulty == actual_difficulty:
                    return write_key

    @staticmethod
    def animal_from_key(write_key):
        return muid.animal(write_key)

    @staticmethod
    def key_difficulty(write_key):
        """
        A measure of key rarity, the difficulty is the length of the memorable part
        """
        nml = muid.animal(write_key)
        return 0 if nml is None else len(nml.replace(' ', ''))

    @staticmethod
    def shash(write_key):
        """ Uses SHA-256 hash to create public identity from private key"""
        # Expects a string not binary
        return muid.shash(write_key)

    @staticmethod
    def animal_from_code(code):
        """ Return spirit animal given public identity (hash of write_key) """
        return muid.search(code=code)

    @staticmethod
    def maybe_create_key(seconds=1, difficulty=11):
        """ Try to mine key subject to timeout
             :param difficulty:  int  minimum length of the memorable part of the hash
             :returns  str or None
        """
        quota = 100000000
        count = 0
        start_time = time.time()
        dffclty = difficulty
        while time.time() - start_time < seconds:
            report, dffclty, count = mine_once(dffclty, count, quota)
            if report:
                return report[0]["key"].decode()

    @staticmethod
    def code_from_code_or_key(code_or_key: str) -> str:
        """ Return hash of key, if key, else return same string """
        if KeyConventions.is_valid_key(code_or_key):
            return KeyConventions.shash(code_or_key)
        elif KeyConventions.animal_from_code(code_or_key):
            return code_or_key

    def key_permission(
            self, activity: Activity, difficulty: int = None, write_key=None
    ) -> bool:
        """ Indicate whether key is difficult enough to permit an activity """
        difficulty = difficulty or self.key_difficulty(write_key=write_key)
        return difficulty >= self.MIN_DIFFICULTIES[activity]

    def permitted(self, **kwargs):
        logging.warning(
            'KeyConventions.permitted is deprecated. Please use '
            'KeyConventions.key_permission.'
        )
        return self.key_permission(**kwargs)

    # ------------------------------------------------------------------------------- #
    #   Special cases of key difficulty permission (for illustration and convenience) #
    # ------------------------------------------------------------------------------- #

    def key_permission_to_set(self, difficulty: int = None, write_key=None):
        """
        Determine whether you or someone else has permission to create stream
        """
        return self.permitted(
            activity=Activity.set, difficulty=difficulty, write_key=write_key
        )

    def key_permission_to_submit(self, difficulty: int = None, write_key=None):
        """
        Determine whether you or someone else has permission to create (update) stream
        """
        return self.permitted(
            activity=Activity.submit, difficulty=difficulty, write_key=write_key
        )

    def key_permission_to_mset(self, difficulty: int = None, write_key=None):
        """
        Determine whether you or someone else has permission to create (update)
        multiple streams
        """
        return self.permitted(
            activity=Activity.mset, difficulty=difficulty, write_key=write_key
        )

    def key_permission_to_cset(self, difficulty: int = None, write_key=None):
        """
        Determine whether you or someone else has permission to create (update)
        multiple streams with implied Copulas
        """
        return self.permitted(
            activity=Activity.mset, difficulty=difficulty, write_key=write_key
        )

    def key_permission_to_give(self, difficulty: int = None, write_key=None):
        """
        Determine whether you or someone else has permission to transfer some
        of your balance
        """
        return self.permitted(
            activity=Activity.give, difficulty=difficulty, write_key=write_key
        )

    def key_permission_to_receive(self, difficulty: int = None, write_key=None):
        """
        Determine whether you or someone else has permission to receive some
        balance
        """
        return self.permitted(
            activity=Activity.receive, difficulty=difficulty, write_key=write_key
        )

    # --------------------- #
    #    Ancestor methods   #
    # --------------------- #
    # (Here to limit code warnings)

    def own_write_key(self):
        """ Retrieve write_key from descendant (e.g. MicroWriter), or raise """
        try:
            return self.write_key
        except AttributeError:
            raise AttributeError(
                'KeyConventions.own_write_key only works for derived classes '
                'with write_key properties'
            )


new_key = KeyConventions.create_key
create_key = KeyConventions.create_key
maybe_create_key = KeyConventions.maybe_create_key
animal_from_key = KeyConventions.animal_from_key
shash = KeyConventions.shash
animal_from_code = KeyConventions.animal_from_code
key_difficulty = KeyConventions.key_difficulty
code_from_code_or_key = KeyConventions.code_from_code_or_key
