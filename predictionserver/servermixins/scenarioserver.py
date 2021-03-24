from predictionserver.futureconventions.leaderboardconventions import LeaderboardConventions, LeaderboardGranularity
from predictionserver.servermixins.memoserver import MemoServer
from pprint import pprint
import time
import numpy as np
from logging import warning
import datetime
import itertools
import math
from collections import Counter


# Scenario server receives requests to submit scenarios, and requests to cancel scenarios


class ScenarioServer(MemoServer):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def submit(self, name, values, delay, write_key, verbose=False):
        return self._permissioned_submit_implementation(
            name=name, values=values, delay=delay, write_key=write_key, verbose=verbose)

    # ------------- #
    #   Retrieving  #
    # ------------- #

    def get_samples(self, name, delay=None, delays=None):
        """ Samples are submitted scenarios that have exited quarantine """
        return self._get_samples_implementation(name=name, delay=delay, delays=delays)

    def get_predictions(self, name, delay=None, delays=None):
        return self._get_predictions_implementation(name=name, delay=delay, delays=delays)

    def get_all_horizon_names(self):
        horizons = list()
        for name in self.get_names():
            for delay in self.DELAYS:
                horizons.append(self.horizon_name(name=name, delay=delay))
        return horizons

    def get_active(self, write_key):
        # FIXME: Crazy inefficient inefficient.
        keys = self.get_all_horizon_names()
        names, delays = self.split_horizon_names(keys)
        active = self.is_active(write_key=write_key, names=names, delays=delays)
        return [k for k, a in zip(keys, active) if a]

    def is_active(self, write_key, names, delays):
        assert len(names) == len(
            delays), "names/delays length mismatch ... maybe use get_active(write_key) instead"
        pipe = self.client.pipeline()
        for name, delay in zip(names, delays):
            pipe.sismember(self._sample_owners_name(name=name, delay=delay), write_key)
        return pipe.execute()

    def _get_predictions_implementation(self, name, delay=None, delays=None, obscure=True):
        return self._get_distribution(
            namer=self._predictions_name,
            name=name,
            delay=delay,
            delays=delays,
            obscure=obscure)

    def _get_samples_implementation(self, name, delay=None, delays=None, obscure=True):
        return self._get_distribution(
            namer=self._samples_name,
            name=name,
            delay=delay,
            delays=delays,
            obscure=obscure)

    def _get_distribution(self, namer, name, delay=None, delays=None, obscure=True):
        """ Get predictions or samples and obfuscate (hash) the write keys """
        singular = delays is None
        delays = delays or [delay]
        distribution_names = [namer(name=name, delay=delay) for delay in delays]
        pipe = self.client.pipeline()
        for distribution_name in distribution_names:
            pipe.zrange(name=distribution_name, start=0, end=-1,
                        withscores=True)  # TODO: Return ordered
        private_distributions = pipe.execute()
        data = list()
        for distribution in private_distributions:
            if obscure:
                _data = dict([(self._make_scenario_obscure(scenario), v)
                             for (scenario, v) in distribution])
            else:
                _data = dict([(scenario, v) for (scenario, v) in distribution])
            data.append(_data)
        return data[0] if singular else data

    # ------------- #
    #   Writing     #
    # ------------- #

    def set_scenarios(self, name, values, delay, write_key, verbose=False):
        warning('set_scenarios is deprecated. Use submit instead')
        return self.submit(
            name=name,
            values=values,
            delay=delay,
            write_key=write_key,
            verbose=verbose)

    def delete_all_scenarios(self, write_key):
        active = self.get_active(write_key=write_key)
        limit = 5
        for horizon in active:
            name, delay = self.split_horizon_name(horizon)
            self._delete_scenarios_implementation(
                name=name, write_key=write_key, delay=delay)
            limit = limit - 1
        return limit > 0

    def delete_scenarios(self, name, write_key, delay=None, delays=None):
        warning('delete_scenarios is deprecated. Use cancel instead. ')
        return self._delete_scenarios_implementation(
            name=name, write_key=write_key, delay=delay, delays=delays)

    def cancel(self, name, write_key, delay=None, delays=None):
        return self._cancel_implementation(
            name=name, write_key=write_key, delay=delay, delays=delays)

    # -------------- #
    #   System use   #
    # -------------- #

    def _cancel_implementation(self, name, write_key, delay=None, delays=None):
        """
            Cancellation cannot occur immediately, as that would allow sneaky people to put in predictions and
            then cancel them later when it seems that they might not perform well.

            Instead, a promise to cancel is created.

            Later, the actual cancellation will occur.

        :param name:         stream name
        :param write_key:
        :param delay:        int
        :param delays:       [ int ]
        :return:
        """
        if delays is None and delay is None:
            delays = self.DELAYS
        elif delays is None:
            delays = [delay]
        assert name == self._root_name(name)
        utc_epoch_now = time.time()
        pipe = self.client.pipeline()
        cancel_queues = list()
        if self.is_valid_key(write_key):
            for delay_seconds in delays:
                cancel_queue = self._cancellation_queue_name(
                    self._cancellation_rounded_time(utc_epoch_now + delay_seconds))
                cancellation = self._cancellation_promise(
                    name=name, delay=delay_seconds, write_key=write_key)
                pipe.sadd(cancel_queue, cancellation)  # (3::3)
                pipe.expire(name=cancel_queue, time=delay_seconds + 10 * self._DELAY_GRACE)
                cancel_queues.append(cancel_queue)
            exec = pipe.execute()
            DEBUG = False
            if DEBUG:
                retrieved = self.client.smembers(cancel_queues[0])
                pprint(cancel_queues)
            expected_exec = [1, True] * len(delays)
            success = all([e1 == e2 for e1, e2 in zip(exec, expected_exec)])
            confirmation = {
                'operation': 'withdraw',
                'time': str(
                    datetime.datetime.now()),
                'name': name,
                'delays': delays,
                'success': success,
                'code': self.shash(write_key),
                'epoch_time': time.time()}
            self._confirm(write_key=write_key, data=confirmation)

            return 1 if all(exec) else 0
        return 0

    def _delete_scenarios_implementation(self, name, write_key, delay=None, delays=None):
        """ Second phase of cancellation
              This is instantaneous deletion ... it is called by the admin when promises to delete are found at the right time.
        """
        if delays is None and delay is None:
            delays = self.DELAYS
        elif delays is None:
            delays = [delay]
        assert name == self._root_name(name)
        if self.is_valid_key(write_key) and all(d in self.DELAYS for d in delays):
            # <-- Important that transaction=True lest submissions and owners get out of sync
            delete_pipe = self.client.pipeline(transaction=True)
            code = self.animal_from_key(write_key)
            confirmation = {
                'operation': 'cancel',
                'time': str(
                    datetime.datetime.now()),
                'success': True,
                'name': name,
                'delays': delays,
                'participant': code,
                'epoch_time': time.time(),
                'explanation': 'delayed withdrawal is complete'}
            for delay in delays:
                collective_predictions_name = self._predictions_name(name, delay)
                keys = [
                    self._format_scenario(
                        write_key,
                        k) for k in range(
                        self.NUM_PREDICTIONS)]
                delete_pipe.zrem(collective_predictions_name, *keys)  # 1000
                samples_name = self._samples_name(name=name, delay=delay)
                delete_pipe.zrem(samples_name, *keys)  # 1000
                owners_name = self._sample_owners_name(name=name, delay=delay)
                delete_pipe.srem(owners_name, write_key)  # 1
            _execut = delete_pipe.execute()

            confirmation.update({'execute': _execut})

            self._confirm(write_key=write_key, data=confirmation)

    def _permissioned_submit_implementation(
            self, name, values, delay, write_key, verbose=False):
        """ Supply scenarios for scalar value taken by name
               values :   [ float ]  len  self.num_predictions
        """
        error_data = {
            'operation': 'submit',
            'success': 0,
            'invalid write key': not self.is_valid_key(write_key),
            'incorrect number of predictions': len(values) != self.num_predictions,
            'not permitted': not self.permitted_to_submit(
                write_key=write_key),
        }
        reasons = ['invalid write key', 'incorrect number of predictions', 'not permitted']
        for reason in reasons:
            if error_data[reason]:
                self._error(write_key=write_key, data=error_data)
                return error_data if verbose else False

        if self.bankrupt(write_key=write_key):
            # Bankruptcy might restrict submission, but not if the balance is close to
            # positive.
            leaderboard = self._get_leaderboard_implementation(
                variety=LeaderboardGranularity.name_and_delay,
                name=name,
                delay=delay,
                readable=False,
                count=10000)
            code = self.shash(write_key=write_key)
            if (code not in leaderboard) or (leaderboard[code] < -1.0):
                error_data.update({'reason': 'bankruptcy'})
                self._error(write_key=write_key, data=error_data)
                return error_data if verbose else False

        fvalues = list(map(float, values))
        return self._set_scenarios_implementation(
            name=name, values=fvalues, delay=delay, write_key=write_key)

    def _set_scenarios_implementation(
            self,
            name,
            values,
            write_key,
            delay=None,
            delays=None,
            verbose=False):
        """ Supply scenarios """
        if delays is None and delay is None:
            delays = self.DELAYS
        elif delays is None:
            delays = [delay]
        assert name == self._root_name(name)
        if len(values) == self.num_predictions and self.is_valid_key(write_key) and all(
                [isinstance(v, (int, float)) for v in values]) and all(delay in self.DELAYS for delay in delays):
            # Ensure sorted ... TODO: force this on the algorithm, or charge a fee?
            values = sorted(values)

            # Jigger sorted predictions
            noise = np.random.randn(self.num_predictions).tolist()
            jiggered_values = [v + n * self.NOISE for v, n in zip(values, noise)]
            jiggered_values.sort()
            if len(set(jiggered_values)) != len(jiggered_values):
                print('----- submission error ----- ')
                num_unique = len(set(values))
                num_jiggled_unique = len(set(values))
                error_message = "Cannot accept submission as there are " + \
                    str(num_unique) + " unique values (" + \
                    str(num_jiggled_unique) + " unique after noise added)"
                warning(error_message)
                print(error_message, flush=True)
                raise Exception(error_message)
            predictions = dict([(self._format_scenario(write_key=write_key, k=k), v)
                                for k, v in enumerate(jiggered_values)])

            # Open pipeline
            set_and_expire_pipe = self.client.pipeline()

            # Add to collective contemporaneous forward predictions
            for delay in delays:
                collective_predictions_name = self._predictions_name(name, delay)
                set_and_expire_pipe.zadd(
                    name=collective_predictions_name,
                    mapping=predictions,
                    ch=True,
                    nx=False)  # [num]*len(delays)

            # Create obscure predictions and promise to insert them later, at
            # different times, into different samples
            utc_epoch_now = int(time.time())
            individual_predictions_name = self._random_promised_name(name)
            set_and_expire_pipe.zadd(
                name=individual_predictions_name,
                mapping=predictions,
                ch=True)  # num
            promise_ttl = max(self.DELAYS) + self._DELAY_GRACE
            set_and_expire_pipe.expire(
                name=individual_predictions_name,
                time=promise_ttl)  # true
            for delay_seconds in delays:
                promise_queue = self._promise_queue_name(utc_epoch_now + delay_seconds)
                promise = self._prediction_promise(
                    target=name, delay=delay_seconds, predictions_name=individual_predictions_name)
                set_and_expire_pipe.sadd(promise_queue, promise)  # (3::3)
                set_and_expire_pipe.expire(
                    name=promise_queue,
                    time=delay_seconds +
                    self._DELAY_GRACE)  # (4::3)
                set_and_expire_pipe.expire(name=individual_predictions_name,
                                           time=delay_seconds + self._DELAY_GRACE)  # (5::3)

            # Execute pipeline ... should not fail (!)
            execut = set_and_expire_pipe.execute()
            anticipated_execut = [self.num_predictions] * \
                len(delays) + [self.num_predictions, True] + [1, True, True] * len(delays)

            def _close(a1, a2):
                return a1 == a2 or (
                    isinstance(
                        a1, int) and a1 > 20 and (
                        (a1 - a2) / self.num_predictions) < 0.05)

            success = all(
                _close(
                    actual,
                    anticipate) for actual,
                anticipate in itertools.zip_longest(
                    execut,
                    anticipated_execut))
            do_warn = not (
                all([a1 == a2 for a1, a2 in itertools.zip_longest(execut, anticipated_execut)]))

            confirmation = {'write_key': write_key,
                            'operation': 'submit',
                            'name': name,
                            'delays': delays,
                            'some_values': values[:5],
                            'success': success,
                            'warn': do_warn}
            if success:
                self._confirm(**confirmation)
            if not success or do_warn:
                confirmation.update(
                    {'antipated_execut': anticipated_execut, 'actual_execut': execut})
                self._error(**confirmation)

            return confirmation if verbose else success
        else:
            # TODO: Log failed prediction attempt to write_key log
            return 0

    def _get_sample_owners(self, name, delay):
        """ Set of participants in a market """
        return list(self.client.smembers(self._sample_owners_name(name=name, delay=delay)))

    def _pools(self, names, delays):
        """ Return count of number of scenarios in predictions::5::name, predictions::1::name  for name in names
               Returns:  pools    { name: [ 5000, 1000, 0, 0 ] }
        """
        pools = dict([(n, list()) for n in names])
        pool_pipe = self.client.pipeline()
        ndxs = dict([(n, list()) for n in names])
        for name in names:
            for delay in delays:
                ndxs[name].append(len(pool_pipe))
                pool_pipe.zcard(self._predictions_name(name=name, delay=delay))
        exec = pool_pipe.execute()
        for name in names:
            for delay_ndx in range(len(delays)):
                pools[name].append(exec[ndxs[name][delay_ndx]])
        return pools

    # --------------------------------------------------------------------------
    #           Settlement
    # --------------------------------------------------------------------------

    def _msettle(
            self,
            names: [str],
            values: [float],
            budgets: [float],
            with_percentiles: bool,
            write_keys: [str],
            with_copulas: bool):
        """ Inspect scenarios submissions that have exited quarantine and issue reward transactions

        :param names:
        :param values:
        :param budgets:
        :param with_percentiles:    bool
        :param write_keys:
        :param with_copulas:

        Returns community implied percentiles for the data point, and also suggested names and values that can
        be subsequently used to create derived z-streams, should that be desirable.

        :return:   dict         'percentiles':dict
                               'z_budgets':[float]
                               'z_names':[str]
                               'values':[float]

        """

        HALF_WINNERS = int(math.ceil(self.NUM_WINNERS))

        assert len(set(names)) == len(names), "mget() cannot be used with repeated names"
        retrieve_pipe = self.client.pipeline()
        num_delay = len(self.DELAYS)
        num_windows = len(self._WINDOWS)
        sponsors = [self.shash(ky) for ky in write_keys]

        # ----  Construct pipe to retrieve quarantined predictions ----------
        scenarios_lookup = dict(
            [(name, dict([(delay_ndx, dict()) for delay_ndx in range(num_delay)])) for name in names])
        pools_lookup = dict([(name, dict()) for name in names])
        participants_lookup = dict([(name, dict()) for name in names])
        for name, value in zip(names, values):
            for delay_ndx, delay in enumerate(self.DELAYS):
                samples_name = self._samples_name(name=name, delay=delay)
                pools_lookup[name][delay_ndx] = len(retrieve_pipe)
                retrieve_pipe.zcard(samples_name)  # Total number of entries
                participants_lookup[name][delay_ndx] = len(retrieve_pipe)
                retrieve_pipe.smembers(
                    self._sample_owners_name(
                        name=name, delay=delay))  # List of owners
                for window_ndx, window in enumerate(self._WINDOWS):
                    scenarios_lookup[name][delay_ndx][window_ndx] = len(retrieve_pipe)
                    retrieve_pipe.zrangebyscore(
                        name=samples_name,
                        min=value,
                        max=value + 0.5 * window,
                        withscores=False,
                        start=0,
                        num=HALF_WINNERS)
                    retrieve_pipe.zrevrangebyscore(
                        name=samples_name,
                        max=value,
                        min=value - 0.5 * window,
                        withscores=False,
                        start=0,
                        num=HALF_WINNERS)
        retrieved = retrieve_pipe.execute()

        # ---- Compute percentiles by zooming out until we have enough points ---
        some_percentiles = False
        percentiles = dict(
            [(name, dict((d, 0.5) for d in range(len(self.DELAYS)))) for name in names])
        if with_percentiles:
            for name in names:
                pools = [retrieved[pools_lookup[name][delay_ndx]]
                         for delay_ndx in range(num_delay)]
                if any(pools):
                    for delay_ndx, pool in enumerate(pools):
                        assert pool == retrieved[pools_lookup[name]
                                                 [delay_ndx]]  # Just checkin
                        participant_set = retrieved[participants_lookup[name][delay_ndx]]
                        if pool and len(participant_set) >= 1:
                            # Zoom out window for percentiles ... want a few so we can average zscores
                            # from more than one contributor, hopefully leading to more
                            # accurate percentiles
                            percentile_scenarios = list()
                            for window_ndx in range(num_windows):
                                if len(percentile_scenarios) < 10:
                                    _ndx = scenarios_lookup[name][delay_ndx][window_ndx]
                                    percentile_scenarios_up = retrieved[_ndx]
                                    percentile_scenarios_dn = retrieved[_ndx + 1]
                                    percentile_scenarios = percentile_scenarios_dn + percentile_scenarios_up
                                    some_percentiles = len(percentile_scenarios) > 0
                            percentiles[name][delay_ndx] = self._zmean_scenarios_percentile(
                                percentile_scenarios=percentile_scenarios)

        # ---- Rewards and leaderboard update pipeline
        pipe = self.client.pipeline()
        pipe.hmset(name=self.BUDGETS, mapping=dict(
            zip(names, budgets)))  # Log the budget decision
        for name, budget, write_key, sponsor, value in zip(
                names, budgets, write_keys, sponsors, values):
            pools = [retrieved[pools_lookup[name][delay_ndx]]
                     for delay_ndx in range(num_delay)]
            if any(pools):
                participant_sets = [retrieved[participants_lookup[name][delay_ndx]]
                                    for delay_ndx in range(num_delay)]
                for delay_ndx, delay, pool, participant_set in zip(
                        range(num_delay), self.DELAYS, pools, participant_sets):
                    payments = Counter()
                    if pool and len(participant_set) > 1:
                        # Zoom out rewards scenarios window if we don't have a winner
                        # Possibly this should be adjusted by the number of participants to reduce wealth variance
                        # It is unlikely that we'd have just one so won't worry too much
                        # about this for now.
                        rewarded_scenarios = list()
                        for window_ndx in range(num_windows):
                            if len(rewarded_scenarios) == 0:
                                _ndx = scenarios_lookup[name][delay_ndx][window_ndx]
                                rewarded_scenarios = retrieved[_ndx]
                                winning_window_ndx = window_ndx
                        winning_window = self._WINDOWS[winning_window_ndx]
                        num_rewarded = len(rewarded_scenarios)

                        game_payments = self._game_payments(
                            pool=pool, participant_set=participant_set, rewarded_scenarios=rewarded_scenarios)
                        payments.update(game_payments)

                    if len(payments):
                        pipe = self.__update_leaderboards_for_horizon(
                            pipe=pipe, name=name, delay=delay, payments=payments, multiplier=budget)
                        leaderboard_names = self.leaderboard_names_to_update(
                            name=name, delay=delay, sponsor=sponsor)
                        write_code = self.shash(write_key)
                        old_leaderboard_names = [
                            self.old_custom_leaderboard_name(
                                sponsor=write_code, name=name), self.old_custom_leaderboard_name(
                                sponsor=write_code)]
                        for (recipient, amount) in payments.items():
                            # Record keeping
                            rescaled_amount = budget * float(amount)
                            self._update_balance(
                                pipe=pipe, write_key=recipient, amount=rescaled_amount)
                            # pipe.hincrbyfloat(name=self._BALANCES, key=recipient, amount=rescaled_amount)
                            pipe.hincrbyfloat(
                                name=self.VOLUMES, key=self.horizon_name(
                                    name=name, delay=delay), amount=abs(rescaled_amount))
                            write_code = self.shash(write_key)
                            recipient_code = self.shash(recipient)
                            # Leaderboards
                            for lb in old_leaderboard_names:
                                pipe.zincrby(
                                    name=lb, value=recipient_code, amount=rescaled_amount)

                            # Performance
                            from predictionserver.futureconventions.performanceconventions import PerformanceGranularity
                            pipe = self.__incr_performance(
                                variety=PerformanceGranularity.write_key,
                                write_key=write_key,
                                name=name,
                                delay=delay,
                                amount=rescaled_amount)
                            pipe.hincrbyfloat(
                                name=self.performance_name_old(
                                    write_key=recipient), key=self.horizon_name(
                                    name=name, delay=delay), amount=rescaled_amount)

                            # Transactions logs:
                            maxed_out = num_rewarded == 2 * HALF_WINNERS
                            mass = num_rewarded / pool if pool > 0.0 else 0.
                            density = mass / winning_window
                            reliable = 0 if maxed_out else 1
                            breakeven = self.num_predictions * num_rewarded / pool if pool > 0 else 0

                            transaction_record = {
                                "settlement_time": str(
                                    datetime.datetime.now()),
                                "amount": rescaled_amount,
                                "budget": budget,
                                "stream": name,
                                "delay": delay,
                                "value": value,
                                "window": winning_window,
                                "mass": mass,
                                "density": density,
                                "average": breakeven,
                                "reliable": reliable,
                                "submissions_count": pool,
                                "submissions_close": num_rewarded,
                                "stream_owner_code": write_code,
                                "recipient_code": recipient_code}
                            log_names = [
                                self.transactions_name(), self.transactions_name(
                                    write_key=recipient), self.transactions_name(
                                    write_key=recipient, name=name), self.transactions_name(
                                    write_key=recipient, name=name, delay=delay)]
                            for ln in log_names:
                                pipe.xadd(name=ln, fields=transaction_record,
                                          maxlen=self.TRANSACTIONS_LIMIT)
                                pipe.expire(name=ln, time=self._TRANSACTIONS_TTL)
                        for lb in leaderboard_names:
                            pipe.expire(name=lb, time=self._LEADERBOARD_TTL)
                            try:
                                # Sometimes leave a note for admin_shrinkage
                                memory = self.leaderboard_memory_from_name(lb)
                                if np.random.rand() < 1. / (memory * self.SHRINKAGE):
                                    pipe.lpush(self._SHRINK_QUEUE, *leaderboard_names)
                            except AttributeError:
                                pass

        settle_exec = pipe.execute()  # No checks here

        result = {"percentiles": percentiles}

        if with_percentiles and some_percentiles:
            first_write_key = write_keys[0]
            # Derived markets use 1-d, 2-d, 3-d market-implied z-scores and z-curves
            z_budgets = list()
            z_names = list()
            z_curves = list()
            z_write_keys = list()
            for delay_ndx, delay in enumerate(self.ZDELAYS()):
                percentiles1 = [percentiles[name][delay_ndx] for name in names]
                legit = not (all([abs(p1 - 0.5) < 1e-5 for p1 in percentiles1]))
                if legit:
                    num_names = len(names)
                    selections = list(itertools.combinations(list(range(num_names)), 1))
                    if with_copulas and num_names <= 10:
                        selections2 = list(
                            itertools.combinations(
                                list(
                                    range(num_names)), 2))
                        selections3 = list(
                            itertools.combinations(
                                list(
                                    range(num_names)), 3))
                        selections = selections + selections2 + selections3

                    for selection in selections:
                        selected_names = [names[o] for o in selection]
                        dim = len(selection)
                        z_budget = self.DERIVED_BUDGET_RATIO * \
                            sum([budgets[o] for o in selection])
                        selected_prctls = [percentiles1[o] for o in selection]
                        zcurve_value = self.to_zcurve(selected_prctls)
                        zname = self.zcurve_name(selected_names, delay)
                        z_names.append(zname)
                        z_budgets.append(z_budget)
                        z_curves.append(zcurve_value)
                        z_write_keys.append(first_write_key)
            if z_names:
                result.update(
                    budgets=z_budgets,
                    names=z_names,
                    values=z_curves,
                    write_keys=z_write_keys)
        return result

    def _zmean_scenarios_percentile(self, percentile_scenarios, included_codes=None):
        """ Each submission has an implicit z-score. Average them. """
        # On the fly discard legacy scenarios where num_predictions are too large
        if included_codes:
            owners_prctls = dict(
                [(self._scenario_owner(s), self._scenario_percentile(s)) for s in percentile_scenarios if
                 (self.shash(self._scenario_owner(s)) in included_codes) and self._scenario_percentile(s) < 1])
        else:
            owners_prctls = dict([(self._scenario_owner(s), self._scenario_percentile(
                s)) for s in percentile_scenarios if self._scenario_percentile(s) < 1])

        prctls = [p for o, p in owners_prctls.items()]
        mean_prctl = self.zmean_percentile(prctls)

        if prctls and included_codes is not None and len(prctls) < len(
                included_codes) and abs(mean_prctl - 0.5) > 1e-6:
            if True:
                E = math.exp(8.0 * (mean_prctl - 0.5))
                missing_prctl = (E / (E + 1))
                prctls = prctls + [missing_prctl] * (len(included_codes) - len(prctls))
            return self.zmean_percentile(prctls)
        else:
            return mean_prctl

    def _game_payments(self, pool, participant_set, rewarded_scenarios):
        if len(rewarded_scenarios) == 0:
            do_carryover = np.random.rand() < 0.05
            if do_carryover:
                game_payments = Counter(dict((p, -1.0) for p in participant_set))
                carryover = Counter({self._RESERVE: 1.0 * pool / self.num_predictions})
                game_payments.update(carryover)
            else:
                game_payments = Counter()
        else:
            game_payments = Counter(dict((p, -1.0) for p in participant_set))
            winners = [self._scenario_owner(ticket) for ticket in rewarded_scenarios]
            # Could augment this to use kernel or whatever
            reward = (1.0 * pool / self.num_predictions) / len(winners)
            payouts = Counter(dict([(w, reward * c) for w, c in Counter(winners).items()]))
            game_payments.update(payouts)

        if abs(sum(game_payments.values())) > 0.1:
            # This can occur if owners gets out of sync with the scenario hash ... which it should not
            # FIXME: Fail gracefully and raise system alert and/or garbage cleanup of
            # owner::samples::delay::name versus samples::delay::name
            print('********************************************')
            print("Leakage in zero sum game", flush=True)
            pprint(game_payments)
            print('********************************************')
            print('Set all payments to zero, for now, but need to fix this...')
            game_payments = Counter(dict((p, 0.0) for p in participant_set))
        return game_payments


if __name__ == '__main__':
    from predictionserver.collider_config_private import REDIZ_COLLIDER_CONFIG, EMBLOSSOM_MOTH
    server = ScenarioServer(**REDIZ_COLLIDER_CONFIG)
    values = [
        server.norminv(p) for p in server.evenly_spaced_percentiles(
            server.num_predictions)]
    server.set_scenarios(
        name='z1~die.json',
        values=values,
        write_key=EMBLOSSOM_MOTH,
        delay=server.DELAYS[0])
