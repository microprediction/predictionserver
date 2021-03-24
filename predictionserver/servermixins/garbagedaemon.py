from predictionserver.servermixins.memoserver import MemoServer
from predictionserver.servermixins.ownershipserver import OwnershipServer


class GarbageDaemon(MemoServer, OwnershipServer):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def admin_garbage_collection(self, fraction=0.1, with_report=False):
        """ Randomized search and destroy for expired streams """
        num_keys = self.client.scard(self._NAMES)
        num_survey = min(100, max(20, int(fraction * num_keys)))
        orphans = self._randomly_find_orphans(num=num_survey)
        if orphans is not None:
            self._delete_implementation(*orphans)
            return len(orphans) if not with_report else {"ophans": orphans}
        else:
            return 0 if not with_report else {"orphans": None}

    def _randomly_find_orphans(self, num=1000):
        """ Find streams that have expired """
        NAMES = self._NAMES()
        unique_random_names = list(set(self.client.srandmember(NAMES, num)))
        num_random = len(unique_random_names)
        if num_random:
            num_exists = self.client.stream_exists(*unique_random_names)
            if num_exists < num_random:
                # There must be orphans, defined as those who are listed
                # in reserved["names"] but have expired
                exists_pipe = self.client.pipeline(transaction=True)
                for name in unique_random_names:
                    exists_pipe.stream_exists(name)
                exists = exists_pipe.execute()

                orphans = [
                    name for name,
                    ex in zip(
                        unique_random_names,
                        exists) if not (ex)]
                return orphans
