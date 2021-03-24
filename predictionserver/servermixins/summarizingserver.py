from predictionserver.futureconventions.leaderboardconventions import LeaderboardGranularity


class SummarizingServer:

    def size(self, name):
        return self._size_implementation(name=name, with_report=False, with_private=True)

    def _size(self, name, with_report=False):  # Move this to diagnostics server
        return self._size_implementation(
            name=name, with_report=with_report, with_private=True)

    def _get_summary_implementation(self, name):
        " Stream summary "

        def delay_level(name, delay):
            things = [
                self.leaderboard_name(
                    variety=LeaderboardVariety.name_and_delay, name=name, delay=delay), self.delayed_name(
                    name=name, delay=delay), self.links_name(
                    name=name, delay=delay)]
            return dict([(thing, get_json_safe(thing=thing, getter=self.get))
                        for thing in things])

        def top_level(name):
            things = [
                name,
                self.lagged_values_name(name),
                self.lagged_times_name(name),
                self.leaderboard_name(
                    granularity=LeaderboardGranularity.name,
                    name=name),
                self.backlinks_name(name),
                self.subscribers_name(name),
                self.subscriptions_name(name),
                self.history_name(name),
                self.messages_name(name)]
            return dict([(thing, shorten(self.get(thing))) for thing in things])

        tl = top_level(name)
        for delay in self.DELAYS:
            tl['delay_' + str(delay)] = delay_level(name=name, delay=delay)
        return tl

    def _get_home_implementation(self, write_key):

        def top_level(write_key):
            things = {
                'code': self.shash(write_key),
                'animal': self.animal_from_key(write_key),
                self.balance_name(
                    write_key=write_key): self.get_balance(
                    write_key=write_key),
                'distance_to_bankruptcy': self.distance_to_bankruptcy(
                    write_key=write_key),
                '/active/' +
                write_key: self.get_active(
                    write_key=write_key),
                self.performance_name(
                    write_key=write_key): self.get_performance(
                    write_key=write_key),
                self.confirms_name(
                    write_key=write_key): self.get_confirms(
                    write_key=write_key),
                self.errors_name(
                    write_key=write_key): self.get_errors(
                    write_key=write_key),
                self.warnings_name(
                    write_key=write_key): self.get_warnings(
                    write_key=write_key),
                self.transactions_name(
                    write_key=write_key): self.get_transactions(
                    write_key=write_key)}
            return dict([(thing, shorten(value, num=10))
                        for thing, value in things.items()])

        return top_level(write_key=write_key)

    def _size_implementation(self, name, with_report=False, with_private=False):
        """ Aggregate memory usage of name and derived names """
        derived = list(self.derived_names(name).values())
        private_derived = list(self._private_derived_names(name).values())
        mem_pipe = self.client.pipeline()
        if with_private:
            all_names = [name] + derived + private_derived
        else:
            all_names = [name] + derived
        for n in all_names:
            mem_pipe.memory_usage(n)
        exec = mem_pipe.execute()
        report_data = list()
        for derived_name, mem in zip(all_names, exec):
            try:
                memory = float(mem)
                report_data.append((derived_name, memory))
            except BaseException:
                pass
        report = dict(report_data)
        total = sum(list(report.values()))
        report.update({"total": total})
        return report if with_report else total

    def get_summary(self, name):
        assert self._root_name(name) == name
        return self._get_summary_implementation(name)

    def get_home(self, write_key):
        return self._get_home_implementation(write_key=write_key)
