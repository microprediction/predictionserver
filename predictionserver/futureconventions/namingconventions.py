from predictionserver.futureconventions.sepconventions import SepConventions
import os
import re
import uuid
import itertools

# Things that should be moved elsewhere :)


class NamingConventions:

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.DELAYED = "delayed" + SepConventions.sep()
        self.CDF = 'cdf' + SepConventions.sep()
        self.LINKS = "links" + SepConventions.sep()
        self.BACKLINKS = "backlinks" + SepConventions.sep()
        self.MESSAGES = "messages" + SepConventions.sep()
        self.HISTORY = "history" + SepConventions.sep()
        self.SUBSCRIBERS = "subscribers" + SepConventions.sep()
        self.SUBSCRIPTIONS = "subscriptions" + SepConventions.sep()
        self.PREDICTIONS = "predictions" + SepConventions.sep()
        self.SAMPLES = "samples" + SepConventions.sep()
        self.BALANCE = "balance" + SepConventions.sep()
        self.PERFORMANCE = "performance" + SepConventions.sep()
        self.BUDGETS = "budget" + SepConventions.sep()
        self.VOLUMES = "volumes" + SepConventions.sep()
        self.SUMMARY = "summary" + SepConventions.sep()

    def history_name(self, name):
        return self.HISTORY + name

    def lagged_values_name(self, name):
        return self.LAGGED_VALUES + name

    def lagged_times_name(self, name):
        return self.LAGGED_TIMES + name

    def links_name(self, name, delay):
        return self.LINKS + str(delay) + SepConventions.sep() + name

    def backlinks_name(self, name):
        return self.BACKLINKS + name

    def subscribers_name(self, name):
        return self.SUBSCRIBERS + name

    def subscriptions_name(self, name):
        return self.SUBSCRIPTIONS + name

    def cdf_name(self, name, delay=None):
        return self.CDF + name if delay is None \
            else self.CDF + str(delay) + SepConventions.sep() + name

    def performance_name(self, write_key):
        return self.PERFORMANCE + write_key + '.json'

    def balance_name(self, write_key):
        return self.BALANCE + write_key + '.json'

    def delayed_name(self, name, delay):
        return self.DELAYED + str(delay) + SepConventions.sep() + name

    def messages_name(self, name):
        return self.MESSAGES + name

    @staticmethod
    def is_plain_name(name: str):
        return NamingConventions.is_valid_name(name) and "~" not in name

    @staticmethod
    def is_valid_name(name: str):
        name_regex = re.compile(r'^[-a-zA-Z0-9_~.:]{1,200}\.[json,html]+$', re.IGNORECASE)
        return (re.match(name_regex, name) is not None) and (
            not SepConventions.sep() in name
        )

    @staticmethod
    def random_name():
        return str(uuid.uuid4()) + '.json'

    def zcurve_names(self, names, delays: [int]):
        znames = list()
        for delay in delays:
            for dim in [1, 2, 3]:
                name_combinations = list(itertools.combinations(sorted(names), dim))
                zname = self.zcurve_name(names=name_combinations, delay=delay)
                znames.append(zname)
        return znames

    @staticmethod
    def zcurve_name(names: [str], delay: int):
        """ Naming convention for derived quantities, called zcurves """
        basenames = sorted([n.split('.')[-2] for n in names])
        prefix = "z" + str(len(names))
        clearbase = "~".join([prefix] + basenames + [str(delay)])
        return clearbase + '.json'


class LegacyNamingConventions:

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.TRANSACTIONS = "transactions" + SepConventions.sep()
        self.CONFIRMS = "confirms" + SepConventions.sep()
        self.WARNINGS = "warnings" + SepConventions.sep()
        self.ERRORS = "errors" + SepConventions.sep()

    def legacy_confirms_name(self, write_key):
        return self.CONFIRMS + write_key + '.json'

    def legacy_errors_name(self, write_key):
        return self.ERRORS + write_key + '.json'

    def legacy_warnings_name(self, write_key):
        return self.WARNINGS + write_key + '.json'

    def legacy_transactions_name(self, write_key=None, name=None, delay=None):
        """ Convention for names of transactions records """
        delay = None if delay is None else str(delay)
        key_stem = None if write_key is None else os.path.splitext(write_key)[0]
        name_stem = None if name is None else os.path.splitext(name)[0]
        tail = SepConventions.sep().join(
            [s for s in [key_stem, delay, name_stem] if s is not None])
        return self.TRANSACTIONS + tail + '.json'
