class SummarizingHabits:

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # Hash of all balances attributed to write_keys
        self._BALANCES = self._obscurity + "balances"

    def _private_delay_methods(self):
        return {
            "participants": self._sample_owners_name,
            "predictions": self._predictions_name,
            "samples": self._samples_name
        }

    def _private_derived_names(self, name):
        references = dict()
        for method_name, method in self._private_delay_methods().items():
            for delay in self.DELAYS:
                item = {
                    method_name + self.SEP + str(delay): method(name=name, delay=delay)
                }
                references.update(item)
        return references

    # --------------------------------------------------------------------------
    #           Public conventions (names and places )
    # --------------------------------------------------------------------------

    def derived_names(self, name):
        """ Summary of data and links  """
        references = dict()
        for method_name, method in self._nullary_methods().items():
            item = {method_name: method(name)}
            references.update(item)
        for method_name, method in self._delay_methods().items():
            for delay in self.DELAYS:
                item = {
                    method_name + self.SEP + str(delay): method(name=name, delay=delay)
                }
                references.update(item)
        return references

    def _nullary_methods(self):
        return {
            "name": self.identity,
            "lagged": self.lagged_values_name,
            "lagged_times": self.lagged_times_name,
            "backlinks": self.backlinks_name,
            "subscribers": self.subscribers_name,
            "subscriptions": self.subscriptions_name,
            "history": self.history_name,
            "messages": self.messages_name}

    def _delay_methods(self):
        return {"delayed": self.delayed_name, "links": self.links_name}
