import sys, math, time, os, uuid
from itertools import zip_longest
import numpy as np
from redis.client import list_or_args
from typing import List, Union, Any, Optional
from microconventions import MicroConventions
from predictionserver.futureconventions.samplers import exponential_bootstrap

KeyList = List[Optional[str]]
NameList = List[Optional[str]]
Value = Union[str, int]
ValueList = List[Optional[Value]]
DelayList = List[Optional[int]]


# TODO: move to microconventions
REDIZ_CONVENTIONS_ARGS = ('history_len', 'delays', 'lagged_len', 'max_ttl', 'error_ttl',
                          'transactions_ttl', 'error_limit', 'windows', 'obscurity',
                          'delay_grace', 'instant_recall')
MICRO_CONVENTIONS_ARGS = ('num_predictions', 'min_len', 'min_balance', 'delays')

from predictionserver.habits.attributehabits import AttributeHabits


class MicroServerConventions(AttributeHabits, MicroConventions):

    def __init__(self, history_len=None, lagged_len=None, delays=None, max_ttl=None, error_ttl=None,
                 transactions_ttl=None,
                 error_limit=None, num_predictions=None, windows=None,
                 obscurity=None, delay_grace=None, instant_recall=None, min_len=None, min_balance=None):

        super().__init__(min_len=min_len, min_balance=min_balance, num_predictions=num_predictions, delays=delays)

        if windows is None:
            windows = [1e-4, 1e-3, 1e-2]
        self.COPY_SEP = self.SEP + "copy" + self.SEP
        self.CANCEL_SEP = self.SEP + "cancel" + self.SEP
        self.PREDICTION_SEP = self.SEP + "prediction" + self.SEP

        # Transparent but parametrized
        self.HISTORY_LEN = int(history_len or 1000)
        self.LAGGED_LEN = int(lagged_len or 10000)

        self.WARNINGS_TTL = int(60 * 60)  # TODO: allow configuation
        self.WARNINGS_LIMIT = 1000
        self.CONFIRMS_TTL = int(error_ttl or 60 * 60)  # Number of seconds that set execution error logs are persisted
        self.ERROR_TTL = int(error_ttl or 24 * 60 * 60)  # Number of seconds that set execution error logs are persisted
        self.CONFIRMS_TTL = int(error_ttl or 60 * 60)  # Number of seconds that set execution error logs are persisted
        self.ERROR_LIMIT = int(error_limit or 1000)  # Number of error messages to keep per write key
        self.CONFIRMS_LIMIT = int(error_limit or 1000)  # Number of error messages to keep per write key
        self.TRANSACTIONS_LIMIT = 1000

        # User transparent temporal and other config
        self.NUM_PREDICTIONS = int(self.num_predictions)  # Number of scenerios in a prediction batch
        self.DELAYS = delays or [1, 5]
        self.ZDELAYS = [self.DELAYS[0], self.DELAYS[-1]]
        self.DERIVED_BUDGET_RATIO = 0.1
        self.CONFIRMS_MAX = 5  # Maximum number of confirmations when using mset()
        self.NUM_WINNERS = 10  # Maximum number of winning tickets
        self.SHRINKAGE = 0.1  # How much to shrink leaderboards

        # Implementation details: private reserved redis keys and prefixes.
        self._obscurity = (obscurity or "obscure") + self.SEP
        self._SHRINK_QUEUE = self._obscurity + "shrink_queue"  # List of leaderboards to shrink
        self._RESERVE = self._obscurity + "reserve"  # Reserve of credits fed by rare cases when all models miss wildly
        #self._OWNERSHIP = self._obscurity + "ownership"  # Official map from name to write_key
        self._BLACKLIST = self._obscurity + "blacklist"  # List of discarded keys
        self._NAMES = self._obscurity + "names"  # Redundant set of all names (needed for random sampling when collecting garbage)
        self._PROMISES = self._obscurity + "promises" + self.SEP  # Prefixes queues of operations that are indexed by epoch second
        self._CANCELLATIONS = self._obscurity + "cancellations" + self.SEP  # Prefixes queues of operations that are indexed by minute
        self._POINTER = self._obscurity + "pointer"  # A convention used in history stream
        self._BALANCES = self._obscurity + "balances"  # Hash of all balances attributed to write_keys
        self._PREDICTIONS = self._obscurity + self.PREDICTIONS  # Prefix to a listing of contemporaneous predictions by horizon. Must be private as this contains write_keys
        #self._OWNERS = "owners" + self.SEP  # Prefix to a redundant listing of contemporaneous prediction owners by horizon. Must be private as this contains write_keys
        self._SAMPLES = self._obscurity + self.SAMPLES  # Prefix to delayed predictions by horizon. Contain write_keys !
        self._PROMISED = "promised" + self.SEP  # Prefixes temporary values referenced by the promise queue
        self._DONATIONS = "donations" + self.SEP  # Write key donations
        self._DONATION_PASSWORD = MicroConventions.shash(self._obscurity)  # Write key donation password
        self._DONORS = "donors"
        self._DISCOUNT = 1.0  # How much get's transfered
        self._REPOS = self._obscurity + "repos"

        # Other implementation config
        self._CANCEL_GRACE = 45
        self._DELAY_GRACE = int(delay_grace or 5)  # Seconds beyond the schedule time when promise data expires
        self._DEFAULT_MODEL_STD = 1.0  # Noise added for self-prediction
        self._WINDOWS = windows  # Sizes of neighbourhoods around truth used in countback ... don't make too big or it hits performance
        self._INSTANT_RECALL = instant_recall or False
        self._MAX_TTL = int(max_ttl or 96 * 60 * 60)  # Maximum TTL, useful for testing
        self._TRANSACTIONS_TTL = int(
            transactions_ttl or 24 * (60 * 60))  # How long to keep transactions stream for inactive write_keys
        self._LEADERBOARD_TTL = int(24 * (60 * 60) * 30)  # How long to keep transactions stream for inactive write_keys
        self._CREATE_COST = 500


    # --------------------------------------------------------------------------
    #           Private conventions (names, places, formats, ttls )
    # --------------------------------------------------------------------------

