import os
import uuid
from predictionserver.futureconventions.sepconventions import SepConventions
from predictionserver.futureconventions.scenarioconventions import ScenarioConventions
from predictionserver.serverhabits.obscurityhabits import ObscurityHabits
from predictionserver.futureconventions.zcurveconventions import ZCurveConventions


class ScenarioHabits(ScenarioConventions, ObscurityHabits, ZCurveConventions):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.COPY_SEP = SepConventions.sep() + "copy" + SepConventions.sep()
        self.CANCEL_SEP = SepConventions.sep() + "cancel" + SepConventions.sep()
        self.PREDICTION_SEP = SepConventions.sep() + "prediction" + SepConventions.sep()
        self.DERIVED_BUDGET_RATIO = 0.1
        self._CANCEL_GRACE = 45
        self._DELAY_GRACE = 5  # Seconds beyond the schedule time when promise data expires
        self.NUM_WINNERS = 10  # Maximum number of winning tickets
        self.SHRINKAGE = 0.1  # How much to shrink leaderboards
        self._WINDOWS = [1e-3, 1e-2, 1e-1, 1, 10]

    def NOISE(self):
        raise Exception('You want TRUTH_NOISE or SAMPLE_NOISE')

    def set_windows(self, windows):
        self._WINDOWS = windows

    def _OWNERS(self):
        # Prefix to a redundant listing of contemporaneous prediction owners by
        # horizon. Must be private as this contains write_keys
        return "owners" + SepConventions.sep()

    def _PROMISED(self):
        # Prefixes temporary values referenced by the promise queue
        return "promised" + self.SEP

    def _PROMISES(self):
        return self.obscurity() + "promises" + SepConventions.sep()

    def _CANCELLATIONS(self):
        # Prefixes queues of operations that are indexed by minute
        return self.obscurity() + "cancellations" + SepConventions.sep()

    def _PREDICTIONS(self):
        # Prefix to a listing of contemporaneous predictions by horizon. Must be
        # private as this contains write_keys
        return self.obscurity() + self.PREDICTIONS

    def _SAMPLES(self):
        # Prefix to delayed predictions by horizon. Contain write_keys !
        return self.obscurity() + self.SAMPLES

    def _random_promised_name(self, name):
        name_stem = os.path.splitext(name)[0]
        return self._PROMISED + \
            str(uuid.uuid4())[:8] + SepConventions.sep() + name_stem + '.json'

    def _copy_promise(self, source, destination):
        return source + self.COPY_SEP + destination

    def _cancellation_queue_name(self, epoch_seconds):
        return self._CANCELLATIONS + str(int(epoch_seconds))

    def _promise_queue_name(self, epoch_seconds):
        return self._PROMISES + str(int(epoch_seconds))

    def _sample_owners_name(self, name, delay):
        return self._OWNERS + self._samples_name(name=name, delay=delay)

    def _predictions_name(self, name, delay):
        return self._PREDICTIONS + str(delay) + SepConventions.sep() + name

    def _samples_name(self, name, delay):
        return self._SAMPLES + str(delay) + SepConventions.sep() + name

    def _format_scenario(self, write_key, k):
        """
        A "ticket" indexed by write_key and an index from 0 to self.NUM_PREDiCTIONS-1
        """
        return str(k).zfill(8) + SepConventions.sep() + write_key

    def _make_scenario_obscure(self, ticket):
        """ Change write_key to a hash of write_key """
        parts = ticket.split(SepConventions.sep())
        return parts[0] + SepConventions.sep() + self.shash(parts[1])

    def _scenario_percentile(self, scenario):
        """ Extract scenario percentile from scenario string """
        return (0.5 + float(
            scenario.split(SepConventions.sep())[0]
        )) / self.num_predictions

    def _scenario_owner(self, scenario):
        """ Extract owner of a scenario from scenario string """
        return scenario.split(SepConventions.sep())[1]

    def _prediction_promise(self, target, delay, predictions_name):
        """
        Format for a promise that sits in a promise queue waiting to be inserted
        into samples::1::name, for instance
        """
        return predictions_name + self.PREDICTION_SEP + \
            self._samples_name(name=target, delay=delay)

    def _cancellation_promise(self, name, delay, write_key):
        """
        Format for a promise to cancel all submitted predictions sits in a
        cancel queue
        """
        return write_key + self.CANCEL_SEP + self.horizon_name(name=name, delay=delay)

    def _interpret_delay(self, delay_name):
        """ Extract root name and delay in seconds as int from  delayed::600:name """
        parts = delay_name.split(SepConventions.sep())
        root = parts[-1]
        delay = int(parts[-2])
        return root, delay

    def percentile_name(self, name, delay):
        return self.zcurve_name(names=[name], delay=delay)
