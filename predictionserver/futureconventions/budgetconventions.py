import muid
import time
from muid.mining import mine_once
from predictionserver.futureconventions.activityconventions import Activity
from predictionserver.futureconventions.keyconventions import KeyConventions


class BudgetConventions:

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.MIN_BALANCE = -1
        self.MIN_DIFFICULTIES = {
            Activity.set: 12,
            Activity.mset: 13,
            Activity.submit: 8,
            Activity.cset: 12,
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
        """
        Create new write_key (string, not bytes)

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

    def bankruptcy_level(self, difficulty: int = None, write_key: str = None):
        """ Key difficulty determines how far below zero a balance can fall """
        write_key = write_key or self.own_write_key()
        difficulty = difficulty or self.key_difficulty(write_key=write_key)
        if difficulty <= 8:
            return -0.001
        return -1.0 * (abs(self.MIN_BALANCE) * (16 ** (difficulty - 9)))

    def maximum_stream_budget(
            self, difficulty: int = None, write_key: str = None
    ) -> float:
        """ Default stream budget, and also maximum budget for yourself or someone else """
        difficulty = difficulty or self.key_difficulty(write_key=write_key)
        budget_1_difficulty = self.MIN_DIFFICULTIES[Activity.mset]
        return abs(
            self.bankruptcy_level(
                difficulty=difficulty
            ) / self.bankruptcy_level(
                difficulty=budget_1_difficulty
            )
        )

    def key_permission(
            self, activity: Activity, difficulty: int = None, write_key=None
    ) -> bool:
        """ Indicate whether key is difficult enough to permit an activity """
        difficulty = difficulty or self.key_difficulty(write_key=write_key)
        return difficulty >= self.MIN_DIFFICULTIES[activity]

    # --------------------- #
    #    Ancestor methods   #
    # --------------------- #
    # (Here to limit code warnings)

    def get_own_balance(self, write_key=None):
        """ Retrieve balance using descendant (e.g. MicroWriter), or raise """
        write_key = write_key or self.own_write_key()
        try:
            return self.get_balance(write_key=write_key)
        except AttributeError:
            raise AttributeError(
                'KeyConventions.own_write_key only works for derived classes '
                'with write_key method.'
            )

    # ---------------------------------------------------------------------------------#
    #    Things that will only work when used in a derived class that has get_balance  #
    # ---------------------------------------------------------------------------------#

    def bankrupt(self, write_key: str = None) -> bool:
        """ Bankruptcy indicator for one's self, or someone else's write key """
        write_key = write_key or self.own_write_key()
        return self.distance_to_bankruptcy(write_key=write_key) < 0

    def distance_to_bankruptcy(
            self, balance: float = None, level: float = None, write_key: str = None
    ) -> float:
        """ Own distance to bankruptcy, or someone else's """
        if balance is None:
            write_key = write_key or self.own_write_key()
            balance = balance or self.get_balance(write_key=write_key)
        level = level or self.bankruptcy_level(write_key=write_key)
        return balance - level


new_key = KeyConventions.create_key
create_key = KeyConventions.create_key
maybe_create_key = KeyConventions.maybe_create_key
animal_from_key = KeyConventions.animal_from_key
shash = KeyConventions.shash
animal_from_code = KeyConventions.animal_from_code
key_difficulty = KeyConventions.key_difficulty
code_from_code_or_key = KeyConventions.code_from_code_or_key
