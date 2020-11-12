from predictionserver.servermixins.laggedserver import LaggedServer
from predictionserver.servermixins.scenarioserver import ScenarioServer


class StreamServer(LaggedServer, ScenarioServer):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)





    def get_budget(self, name):
        return self.client.hget(name=self.BUDGETS, key=name)

    def get_repository(self, write_key):
        """ Accepts write_key or code """
        return self.client.hget(name=self._REPOS, key=self.shash(write_key)) if self.is_valid_key(
            write_key) else self.client.hget(name=self._REPOS, key=write_key)

    def get_budgets(self):
        return self._descending_values(self.client.hgetall(name=self.BUDGETS))




    # --------------------------------------------------------------------------
    #            Public interface  (set/delete streams)
    # --------------------------------------------------------------------------

    def mtouch(self, names, write_key, operation='mtouch'):
        if self.permitted_to_mset(write_key=write_key):
            budgets = [self.maximum_stream_budget(write_key=write_key) for _ in names]
            return self._mtouch_implementation(names=names, write_key=write_key, budgets=budgets)
        else:
            error_data = {'operation': operation, 'message': 'Not permitted with write_key supplied', 'success': False}
            self._error(write_key=write_key, data=error_data)
            return error_data

    def touch(self, name: str, write_key: str):
        return self.mtouch(names=[name], write_key=write_key)

    def set(self, name, value, write_key, budget=1.):
        """ Set name=value and initiate clearing, derived zscore market etc """
        memo = Memo(activity=Activity.set, genre=ActivityContext)
        assert MicroServerConventions.is_plain_name(name), "Expecting plain name"
        assert MicroServerConventions.is_valid_key(write_key), "Invalid write_key"
        if not self.permitted_to_set(write_key=write_key):
            reason = "Write key isn't sufficiently rare to create or update a stream"
            self._error(write_key=write_key, operation='set', success=False, reason=reason)
            return False
        else:
            budget = min(self.maximum_stream_budget(write_key=write_key), budget)
            return self._mset_implementation(name=name, value=value, write_key=write_key, return_args=None,
                                             budget=budget, with_percentiles=True)

    def cset(self, names: NameList, values: ValueList, write_key: str, budget: float = 1.0):
        if not self.permitted_to_cset(write_key=write_key):
            reason = "Write key isn't sufficiently rare to request joint distributions."

            self._error(write_key=write_key, operation='set', success=False, reason=reason)
            return False
        else:
            budget = min(budget, self.maximum_stream_budget(write_key=write_key))
            budgets = [budget / len(names) for _ in names]
            return self._mset(names=names, values=values, budgets=budgets, write_key=write_key, with_copulas=True)

    def mset(self, names: NameList, values: ValueList, write_key: str, budget: float = 1.0):
        if not self.permitted_to_mset(write_key=write_key) or len(names) > 10:
            reason = "Write key isn't sufficiently rare to request mset, or too many names."
            self._error(write_key=write_key, operation='mset', success=False, reason=reason)
            return False
        else:
            budget = min(budget, self.maximum_stream_budget(write_key=write_key))
            budgets = [budget / len(names) for _ in names]
            return self._mset(names=names, values=values, budgets=budgets, write_key=write_key, with_copulas=False)

    def _mset(self, names: NameList, values: ValueList, budgets: List[float], write_key=None, write_keys=None,
              with_copulas=False):
        """ Apply set() for multiple names and values, with copula derived streams optionally """  # Todo: disallow calling with multiple write_keys
        is_plain = [MicroServerConventions.is_plain_name(name) for name in names]
        if not len(names) == len(values):
            error_data = {'names': names, 'values': values, 'message': 'Names and values have different lengths'}
            self._error(write_key=write_key, data=error_data)
            raise Exception(json.dumps(error_data))
        if not all(is_plain):
            culprits = [n for n, isp in zip(names, is_plain) if not (isp)]
            error_data = {'culprits': culprits,
                          'error': 'One or more names are not considered plain names. See MicroConvention.is_plain_name '}
            self._error(write_key=write_key, data=error_data)
            raise Exception(json.dumps(error_data))
        else:
            write_keys = write_keys or [write_key for _ in names]
            return self._mset_implementation(names=names, values=values, write_keys=write_keys, return_args=None,
                                             budgets=budgets, with_percentiles=True, with_copulas=with_copulas)

    def delete(self, name, write_key):
        """ Delete/expire all artifacts associated with name (links, subs, markets etc) """
        return self._permissioned_mdelete(name=name, write_key=write_key)

    def mdelete(self, names, write_key: Optional[str] = None, write_keys: Optional[KeyList] = None):
        """ Delete/expire all artifacts associated with multiple names """
        return self._permissioned_mdelete(names=names, write_key=write_key, write_keys=write_keys)

    # --------------------------------------------------------------------------
    #            Public interface  (set/delete scenarios)
    # --------------------------------------------------------------------------


    # --------------------------------------------------------------------------
    #            Implementation  (set)
    # --------------------------------------------------------------------------

    def _mset_implementation(self, names: Optional[NameList] = None, values: Optional[ValueList] = None,
                             write_keys: Optional[KeyList] = None, budgets: Optional[List[float]] = None,
                             name: Optional[str] = None, value: Optional[Any] = None, write_key: Optional[str] = None,
                             budget: Optional[float] = None,
                             return_args: Optional[List[str]] = None, with_percentiles=False, with_copulas=False):

        if return_args is None:
            return_args = ['name', 'write_key', 'value', 'percentile']
        names, values, write_keys, budgets = MicroServerConventions.coerce_inputs(names=names, values=values,
                                                                                  write_keys=write_keys, budgets=budgets,
                                                                                  name=name, value=value, write_key=write_key,
                                                                                  budget=budget)
        singular = len(names) == 1

        # Convert from objects if not redis native ... this includes vectors
        values = [v if isinstance(v, (int, float, str)) else json.dumps(v) for v in values]

        # Execute assignment (creates temporary execution logs)
        execution_log = self._pipelined_set(names=names, values=values, write_keys=write_keys, budgets=budgets)

        # Ensure there is at least one (truly shitty) baseline prediction and occasionally update it
        pools = self._pools(names, self.DELAYS)
        for nm, v, wk in zip(names, values, write_keys):
            if self.is_scalar_value(v):
                for delay_ndx, delay in enumerate(self.DELAYS):
                    if np.random.rand() < 1 / 20 and pools[nm][delay_ndx] < 4:
                        self._baseline_prediction(name=nm, value=v, write_key=wk, delay=delay)
                    elif pools[nm][delay_ndx] >= 3:
                        # We can step aside now that two others are fighting it out.
                        self._cancel_implementation(name=nm, write_key=wk, delay=delay)

        # Rewards, percentiles ... but only for scalar floats
        # Settlement also triggers the derived market for zscores
        if all(self.is_scalar_value(v) for v in values):
            fvalues = list(map(float, values))
            prctls = self._msettle(names=names, values=fvalues, budgets=budgets, with_percentiles=with_percentiles,
                                   write_keys=write_keys, with_copulas=with_copulas)
        else:
            prctls = None

        # Coerce execution log and maybe add percentiles
        exec_args = [arg for arg in return_args if arg in ['name', 'write_key', 'value']]
        titles = self._coerce_outputs(execution_log=execution_log, exec_args=exec_args)
        if prctls is not None:
            for title in titles:
                if title["name"] in prctls:
                    title.update({"percentiles": prctls[title["name"]]})

        # Write to confirmation log
        self._confirm(write_key=write_keys[0], operation='set', count=len(titles or []), examples=titles[:2])

        return titles[0] if singular else titles

    def _pipelined_set(self, names, values, write_keys, budgets):
        """ Parallel assignment and some knock-on effects of clearing (rewards, derived market) """
        ndxs = list(range(len(names)))
        executed_obscure, rejected_obscure, ndxs, names, values, write_keys = self._pipelined_set_obscure(ndxs=ndxs,
                                                                                                          names=names,
                                                                                                          values=values,
                                                                                                          write_keys=write_keys,
                                                                                                          budgets=budgets)
        executed_new, rejected_new, ndxs, names, values, write_keys = self._pipelined_set_new(ndxs=ndxs, names=names,
                                                                                              values=values,
                                                                                              write_keys=write_keys,
                                                                                              budgets=budgets)
        executed_existing, rejected_existing = self._pipelined_set_existing(ndxs=ndxs, names=names, values=values,
                                                                            write_keys=write_keys, budgets=budgets)
        executed = executed_obscure + executed_new + executed_existing

        # Propagate to subscribers
        modified_names = [ex["name"] for ex in executed]
        modified_values = [ex["value"] for ex in executed]
        self._propagate_to_subscribers(names=modified_names, values=modified_values)
        return {"executed": executed, "rejected": rejected_obscure + rejected_new + rejected_existing}

    @staticmethod
    def _coerce_outputs(execution_log, exec_args=None):
        """ Convert to list of dicts containing names and write keys """
        if exec_args is None:
            exec_args = ('name', 'write_key')
        sorted_log = sorted(execution_log["executed"] + execution_log["rejected"], key=lambda d: d['ndx'])
        return [dict((arg, s[arg]) for arg in exec_args) for s in sorted_log]

    def _pipelined_set_obscure(self, ndxs, names, values, write_keys, budgets):
        # Set values only if names were None. Random names will be assigned.
        executed = list()
        rejected = list()
        ignored_ndxs = list()
        if ndxs:
            obscure_pipe = self.client.pipeline(transaction=True)

            for ndx, name, value, write_key, budget in zip(ndxs, names, values, write_keys, budgets):
                if not (self.is_valid_value(value)):
                    rejected.append({"ndx": ndx, "name": name, "write_key": None, "value": value,
                                     "error": "invalid value of type " + str(type(value)) + " was supplied"})
                else:
                    if name is None:
                        if not (self.is_valid_key(write_key)):
                            rejected.append(
                                {"ndx": ndx, "name": name, "write_key": None, "errror": "invalid write_key"})
                        else:
                            new_name = self.random_name()
                            ttl = self._cost_based_ttl(value=value, budget=budget)
                            obscure_pipe, intent = self._new_obscure_page(pipe=obscure_pipe, ndx=ndx, name=new_name,
                                                                          value=value, write_key=write_key,
                                                                          budget=budget)
                            executed.append(intent)
                    elif not (self.is_valid_name(name)):
                        rejected.append({"ndx": ndx, "name": name, "write_key": None, "error": "invalid name"})
                    else:
                        ignored_ndxs.append(ndx)

            if len(executed):
                obscure_results = MicroServerConventions.chunker(results=obscure_pipe.execute(), n=len(executed))
                for intent, res in zip(executed, obscure_results):
                    intent.update({"result": res})

        # Marshall residual. Return indexes, names, values and write_keys that are yet to be processed.
        names = [n for n, ndx in zip(names, ndxs) if ndx in ignored_ndxs]
        values = [v for v, ndx in zip(values, ndxs) if ndx in ignored_ndxs]
        write_keys = [w for w, ndx in zip(write_keys, ndxs) if ndx in ignored_ndxs]
        return executed, rejected, ignored_ndxs, names, values, write_keys

    def _pipelined_set_new(self, ndxs, names, values, write_keys, budgets):
        # Treat cases where name does not exist yet
        executed = list()
        rejected = list()
        ignored_ndxs = list()

        if ndxs:
            exists_pipe = self.client.pipeline(transaction=False)
            fakeredis = self.client.connection is None
            for name in names:
                exists_pipe.hexists(name=self._ownership_name(), key=name)
            exists = exists_pipe.execute()

            new_pipe = self.client.pipeline(transaction=False)
            for exist, ndx, name, value, write_key, budget in zip(exists, ndxs, names, values, write_keys, budgets):
                if not (exist):
                    if not (self.is_valid_key(write_key)):
                        rejected.append({"ndx": ndx, "name": name, "write_key": None, "errror": "invalid write_key"})
                    else:
                        ttl = self._cost_based_ttl(value=value, budget=budget)
                        new_pipe, intent = self._new_page(new_pipe, ndx=ndx, name=name, value=value,
                                                          write_key=write_key, budget=budget, fakeredis=fakeredis)
                        executed.append(intent)
                else:
                    ignored_ndxs.append(ndx)

            if len(executed):
                new_results = MicroServerConventions.chunker(results=new_pipe.execute(), n=len(executed))
                for intent, res in zip(executed, new_results):
                    intent.update({"result": res})

        # Return those we are yet to get to because they are not new
        names = [n for n, ndx in zip(names, ndxs) if ndx in ignored_ndxs]
        values = [v for v, ndx in zip(values, ndxs) if ndx in ignored_ndxs]
        write_keys = [w for w, ndx in zip(write_keys, ndxs) if ndx in ignored_ndxs]
        return executed, rejected, ignored_ndxs, names, values, write_keys

    def _pipelined_set_existing(self, ndxs, names, values, write_keys, budgets):
        # Potentially modify existing name, assuming write_keys are correct
        executed = list()
        rejected = list()
        if ndxs:
            modify_pipe = self.client.pipeline(transaction=False)
            error_pipe = self.client.pipeline(transaction=False)
            official_write_keys = self._mauthority(names)
            for ndx, name, value, write_key, official_write_key, budget in zip(ndxs, names, values, write_keys,
                                                                               official_write_keys, budgets):
                if write_key == official_write_key:
                    modify_pipe, intent = self._modify_page(modify_pipe, ndx=ndx, name=name, value=value, budget=budget)
                    intent.update({"ndx": ndx, "write_key": write_key})
                    executed.append(intent)
                else:
                    auth_message = {"ndx": ndx, "name": name, "value": value, "write_key": write_key,
                                    "official_write_key_ends_in": official_write_key[-4:],
                                    "error": "write_key does not match page_key on record"}
                    intent = auth_message
                    error_pipe.lpush(self.errors_name(write_key=write_key), json.dumps(auth_message))
                    error_pipe.expire(self.errors_name(write_key=write_key), self.ERROR_TTL)
                    error_pipe.ltrim(name=self.errors_name(write_key=write_key), start=0, end=self.ERROR_LIMIT)
                    rejected.append(intent)
            if len(executed):
                modify_results = MicroServerConventions.chunker(results=modify_pipe.execute(), n=len(executed))
                for intent, res in zip(executed, modify_results):
                    intent.update({"result": res})

            if len(rejected):
                error_pipe.execute()

        return executed, rejected

    def _propagate_to_subscribers(self, names, values):
        """ Create a message for every subscriber """
        subscriber_pipe = self.client.pipeline(transaction=False)
        for name in names:
            subscriber_pipe.smembers(name=self.subscribers_name(name=name))
        subscribers_sets = subscriber_pipe.execute()
        propagate_pipe = self.client.pipeline(transaction=False)

        executed = list()
        for sender_name, value, subscribers_set in zip(names, values, subscribers_sets):
            for subscriber in subscribers_set:
                mailbox_name = self.messages_name(subscriber)
                propagate_pipe.hset(name=mailbox_name, key=sender_name, value=value)
                executed.append({"mailbox_name": mailbox_name, "sender": sender_name, "value": value})

        if len(executed):
            propagation_results = MicroServerConventions.chunker(results=propagate_pipe.execute(), n=len(executed))
            for intent, res in zip(executed, propagation_results):
                intent.update({"result": res})

        return executed

    def _new_obscure_page(self, pipe, ndx, name, value, write_key, budget, fakeredis=False):
        """ Almost the same as a new page """
        pipe, intent = self._new_page(pipe=pipe, ndx=ndx, name=name, value=value, write_key=write_key, budget=budget,
                                      fakeredis=fakeredis)
        intent.update({"obscure": True})
        return pipe, intent

    def _new_page(self, pipe, ndx, name, value, write_key, budget, fakeredis=False):
        """ Create new page:
              pipe         :  Redis pipeline that will be modified
            Returns also:
              intent       :  Explanation log in form of a dict
        """
        # Establish ownership
        pipe.hset(name=self._ownership_name(), key=name, value=write_key)
        pipe.sadd(self._NAMES, name)
        # Charge for stream creation
        create_charge = -budget * self._CREATE_COST
        transaction_record = {"settlement_time": str(datetime.datetime.now()), "type": "create",
                              "write_key": write_key, "amount": create_charge,
                              "message": "charge for new stream creation", "name": name}
        pipe.hincrbyfloat(name=self._BALANCES, key=write_key, amount=create_charge)
        if not fakeredis:
            log_names = [self.transactions_name(write_key=write_key),
                         self.transactions_name(write_key=write_key, name=name)]
            for ln in log_names:
                pipe.xadd(name=ln, fields=transaction_record, maxlen=self.TRANSACTIONS_LIMIT)
                pipe.expire(name=ln, time=self._TRANSACTIONS_TTL)
        # Then modify
        pipe, intent = self._modify_page(pipe=pipe, ndx=ndx, name=name, value=value, budget=budget)
        intent.update({"new": True, "write_key": write_key, "value": value})
        return pipe, intent

    def _modify_page(self, pipe, ndx, name, value, budget):
        """ Create pipelined operations for save, buffer, history etc """
        # Remark: It is important the exactly the same number of redis operations are used
        # here regardless of how things branch, because this simplifies considerably the
        # unpacking of pipelined results in calling algorithms.

        # (1) Set the actual value ... which will be overwritten by the next set() ... and a randomly named copy that survives longer
        ttl = self._cost_based_ttl(value=value, budget=budget)
        pipe.set(name=name, value=value, ex=ttl)
        name_of_copy = self._random_promised_name(name)
        promise_ttl = self._promise_ttl()
        pipe.set(name=name_of_copy, value=value, ex=promise_ttl)

        # (1.5) Update the time to live for predictions and samples
        distribution_ttl = self._cost_based_distribution_ttl(budget=budget)
        for delay in self.DELAYS:
            pipe.expire(name=self._samples_name(name=name, delay=delay), time=distribution_ttl)
            pipe.expire(name=self._sample_owners_name(name=name, delay=delay), time=distribution_ttl)
            pipe.expire(name=self._predictions_name(name=name, delay=delay), time=distribution_ttl)

        # (2) Decide how to store: lags, history or neither, but always use exactly six operations
        len_in = len(pipe)
        good_for_lags = self.is_scalar_value(value)
        if not good_for_lags:
            if self.is_small_value(value):
                if self.is_vector_value(value):
                    good_for_lags = True
        if good_for_lags:
            # Dynamically choose length of lags according to size of value
            t = time.time()
            lag_len = self._cost_based_lagged_len(value)
            lv = self.lagged_values_name(name)
            lt = self.lagged_times_name(name)
            pipe.lpush(lv, value)
            pipe.lpush(lt, t)
            pipe.ltrim(name=lv, start=0, end=lag_len)
            pipe.ltrim(name=lt, start=0, end=lag_len)
            pipe.expire(lv, ttl)
            pipe.expire(lt, ttl)

        # Other types value field(s) may be stored in stream instead ... (note again: exactly six operations so chunking of pipeline is trivial)
        if not good_for_lags:
            if self._streams_support():
                if self.is_small_value(value):
                    fields = MicroServerConventions.to_record(value)
                else:
                    fields = {self._POINTER: name_of_copy}
                history = self.history_name(name)
                history_len = self._cost_based_history_len(value=fields)
                pipe.xadd(history, fields=fields)
                pipe.xtrim(history, maxlen=history_len)
                pipe.expire(history, ttl)
                pipe.expire(name=name_of_copy, time=ttl)
                pipe.expire(name=name_of_copy, time=ttl)  # 5th operation
                pipe.expire(name=name_of_copy, time=ttl)  # 6th operation
            else:
                for _ in range(6):  # Again ... same hack ... insist on (6) operations here
                    pipe.expire(name=name_of_copy, time=promise_ttl)
        len_out = len(pipe)
        assert len_out - len_in == 6, "Need precisely six operations so parent function can chunk pipeline results"

        # (4) Construct delay promises
        utc_epoch_now = int(time.time())
        for delay in self.DELAYS:
            queue = self._promise_queue_name(utc_epoch_now + delay)  # self.PROMISES+str(utc_epoch_now+delay)
            destination = self.delayed_name(name=name, delay=delay)  # self.DELAYED+str(delay_seconds)+self.SEP+name
            promise = self._copy_promise(source=name_of_copy, destination=destination)
            pipe.sadd(queue, promise)
            pipe.expire(name=queue, time=promise_ttl)

        # (5) Execution log
        intent = {"ndx": ndx, "name": name, "value": value, "ttl": ttl, "new": False, "obscure": False,
                  "copy": name_of_copy}

        return pipe, intent

    # --------------------------------------------------------------------------
    #            Implementation  (delete)
    # --------------------------------------------------------------------------

    def _permissioned_mdelete(self, name=None, write_key=None, names: Optional[NameList] = None,
                              write_keys: Optional[KeyList] = None):
        """ Permissioned delete """
        names = names or [name]
        self.assert_not_in_reserved_namespace(names)
        write_keys = write_keys or [write_key for _ in names]
        are_valid = self._mauthorize(names, write_keys)

        authorized_kill_list = [name for (name, is_valid_write_key) in zip(names, are_valid) if is_valid_write_key]
        if authorized_kill_list:
            return self._delete_implementation(*authorized_kill_list)
        else:
            return 0

    def _delete_z1_implementation(self, names):
        """ Delete z1~, z2~ names """
        for name in names:
            znames = [self.zcurve_name(names=[name], delay=delay) for delay in self.DELAYS]
            for zname in znames:
                self._delete_implementation(zname)

    def _delete_implementation(self, names, *args):
        """ Removes all traces of name (except leaderboards) """

        names = list_or_args(names, args)
        names = [n for n in names if n is not None]

        # (a) Gather and assemble stream "edges"  (links, backlinks, subscribers, subscriptions)
        info_pipe = self.client.pipeline()
        for name in names:
            info_pipe.smembers(self.subscribers_name(name))
        for name in names:
            info_pipe.smembers(self.subscriptions_name(name))
        for name in names:
            info_pipe.hgetall(self.backlinks_name(name))
        links_ndx = dict([(k, dict()) for k in range(len(names))])
        for name_ndx, name in enumerate(names):
            for delay_ndx, delay in enumerate(self.DELAYS):
                links_ndx[name_ndx][delay_ndx] = len(info_pipe)
                info_pipe.hgetall(self.links_name(name=name, delay=delay))

        info_exec = info_pipe.execute()
        assert len(info_exec) == 3 * len(names) + len(names) * len(self.DELAYS)
        subscribers_res = info_exec[:len(names)]
        subscriptions_res = info_exec[len(names):2 * len(names)]
        backlinks_res = info_exec[2 * len(names):]

        # (b)   Second call will do all remaining cleanup
        delete_pipe = self.client.pipeline(transaction=False)

        # (b-1) Force backlinkers to unlink
        for name, backlinks in zip(names, backlinks_res):
            for backlink in list(backlinks.keys()):
                root, delay = self._interpret_delay(backlink)
                delete_pipe = self._unlink_pipe(pipe=delete_pipe, name=root, delay=int(delay), target=name)

        # (b-2) Force subscribers to unsubscribe
        for name, subscribers in zip(names, subscribers_res):
            for subscriber in subscribers:
                delete_pipe = self._unsubscribe_pipe(pipe=delete_pipe, name=subscriber, source=name)

        # (b-3) Unsubscribe gracefully
        for name, sources in zip(names, subscriptions_res):
            delete_pipe = self._unsubscribe_pipe(pipe=delete_pipe, name=name, sources=sources)

        # (b-4) Unlink gracefully
        for name_ndx, name in enumerate(names):
            for delay_ndx, delay in enumerate(self.DELAYS):
                link_ndx = links_ndx[name_ndx][delay_ndx]
                targets = list(info_exec[link_ndx].keys())
                if targets:
                    for target in targets:
                        delete_pipe = self._unlink_pipe(pipe=delete_pipe, name=name, delay=delay, target=target)

        # (b-5) Then discard derived ... delete can be slow so we expire instead
        for name in names:
            derived_names = list(self.derived_names(name).values()) + list(self._private_derived_names(name).values())
            for derived_name in derived_names:
                delete_pipe.pexpire(name=derived_name, time=1)

        # (b-7) Delete budget
        delete_pipe.hdel(self.BUDGETS, *names)
        delete_pipe.hdel(self.VOLUMES, *names)

        # (b-6) And de-register the name
        delete_pipe.srem(self._NAMES, *names)
        delete_pipe.hdel(self._ownership_name(), *names)

        # TODO: Expire the leaderboards also

        del_exec = delete_pipe.execute()

        return sum((1 for r in del_exec if r))

    # --------------------------------------------------------------------------
    #            Implementation  (touch)
    # --------------------------------------------------------------------------



    def _touch_implementation(self, name, write_key, budget, example_value=3.145):
        """ Extend life of stream """
        execut = self.client.expire(name=name, time=self._cost_based_ttl(value=example_value, budget=budget))
        self._confirm(write_key=write_key, operation='touch', name=name, execution=execut)
        if not execut:
            self._warn(write_key=write_key, operation='touch', error='expiry not set ... names may not exist',
                       name=name, exec=execut)
        return execut

    def _mtouch_implementation(self, names, write_key, budgets, example_value=3.145):
        """ Extend life of multiple streams """
        ttls = [self._cost_based_ttl(value=example_value, budget=b) for b in budgets]

        expire_pipe = self.client.pipeline()
        for name, ttl in zip(names, ttls):
            dn = self.derived_names(name=name)
            pdn = self._private_derived_names(name=name)
            all_names = [name] + list(dn.values()) + list(pdn.values())
            for nm in all_names:
                expire_pipe.expire(name=nm, time=ttl)
        exec = expire_pipe.execute()
        report = dict(zip(all_names, exec))
        self._confirm(write_key=write_key, operation='mtouch', count=sum(exec))
        if not any(exec):
            self._warn(write_key=write_key, operation='mtouch', error='expiry not set ... names may not exist',
                       data=report, ttls=ttls)
        return sum(exec)

    def _copula_touch_implementation(self, names, budgets):
        return False  # TODO

    # --------------------------------------------------------------------------
    #            Implementation  (subscribe)
    # --------------------------------------------------------------------------



    # --------------------------------------------------------------------------
    #      Implementation  (Admministrative - garbage collection )
    # --------------------------------------------------------------------------





    # --------------------------------------------------------------------------
    #            Implementation  (Administrative - promises)
    # --------------------------------------------------------------------------

    def _cancellation_rounded_time(self, t):
        return int(round(t, -2))

    def admin_cancellations(self, with_report=False):
        """
              Look at queues of requests to cancel predictions, and process them
        """

        # Find recent cancellation queues that exist
        exists_pipe = self.client.pipeline()
        utc_epoch_now = int(time.time())
        candidates = list(set(
            [self._cancellation_queue_name(epoch_seconds=self._cancellation_rounded_time(utc_epoch_now - seconds)) for
             seconds in range(self._CANCEL_GRACE, -1, -1)]))
        for candidate in candidates:
            exists_pipe.stream_exists(candidate)
        exists = exists_pipe.execute()

        # Get them if they exist
        get_pipe = self.client.pipeline()
        cancellation_queue_name = [promise for promise, exist in zip(candidates, exists) if exists]
        for collection_name in cancellation_queue_name:
            get_pipe.smembers(collection_name)
        cancel_collections = get_pipe.execute()
        self.client.delete(*cancellation_queue_name)

        # Perform cancellations
        individual_cancellations = list(itertools.chain(*cancel_collections))
        for cancellation in individual_cancellations:
            if self.CANCEL_SEP in cancellation:
                write_key, horizon = cancellation.split(self.CANCEL_SEP)
                name, delay = self.split_horizon_name(horizon)
                self._delete_scenarios_implementation(name=name, write_key=write_key, delay=delay)

        return sum(individual_cancellations) if not with_report else {'cancellations': individual_cancellations}


    # --------------------------------------------------------------------------
    #            Implementation  (prediction and settlement)
    # --------------------------------------------------------------------------

    def _baseline_prediction(self, name, value, write_key, delay):
        # So bad !!!
        lagged_values = self._get_lagged_implementation(name, with_times=False, with_values=True, to_float=True,
                                                        start=0, end=None, count=self.num_predictions)
        predictions = self.empirical_predictions(lagged_values=lagged_values)
        return self._set_scenarios_implementation(name=name, values=predictions, write_key=write_key, delay=delay)










    @staticmethod
    def _flatten(list_of_lists):
        return [item for sublist in list_of_lists for item in sublist]






class StreamServer(SummarizingHabits):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

