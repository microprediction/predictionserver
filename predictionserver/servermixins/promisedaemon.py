from predictionserver.servermixins.memoserver import MemoServer
from predictionserver.servermixins.ownershipserver import OwnershipServer
import time
import itertools
from redis.exceptions import DataError


class PromiseDaemon(MemoServer, OwnershipServer):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def admin_promises(self, with_report=False):
        """ Iterate through task queues populating delays and samples """

        # Find recent promise queues that exist
        exists_pipe = self.client.pipeline()
        utc_epoch_now = int(time.time())
        candidates = [
            self._promise_queue_name(
                epoch_seconds=utc_epoch_now - seconds
            ) for seconds in range(self._DELAY_GRACE, -1, -1)
        ]
        for candidate in candidates:
            exists_pipe.stream_exists(candidate)
        exists = exists_pipe.execute()

        # If they exist get the members
        get_pipe = self.client.pipeline()
        promise_collection_names = [
            promise for promise, exist in zip(candidates, exists) if exists
        ]
        for collection_name in promise_collection_names:
            get_pipe.smembers(collection_name)
        collections = get_pipe.execute()
        # Immediately delete task list so it isn't done twice ... not that that would
        self.client.delete(*promise_collection_names)
        # be the end of the world
        individual_promises = list(itertools.chain(*collections))

        # Sort through promises in reverse time precedence
        # In particular, we allow more recent copy instructions to override less
        # recent ones
        dest_source = dict()
        dest_method = dict()
        for promise in individual_promises:
            if self.COPY_SEP in promise:
                source, destination = promise.split(self.COPY_SEP)
                dest_source[destination] = source
                dest_method[destination] = 'copy'
            elif self.PREDICTION_SEP in promise:
                source, destination = promise.split(self.PREDICTION_SEP)
                dest_source[destination] = source
                dest_method[destination] = 'predict'
            else:
                raise Exception("invalid promise")

        sources = list(dest_source.values())
        destinations = list(dest_source.keys())
        methods = list(dest_method.values())

        # Interpret the promises as source / destination references and get the
        # source values
        retrieve_pipe = self.client.pipeline()
        for source, destination, method in zip(sources, destinations, methods):
            if method == 'copy':
                retrieve_pipe.get(source)
            elif method == 'predict':
                retrieve_pipe.zrange(name=source, start=0, end=-1, withscores=True)
            elif method == 'cancel':
                retrieve_pipe.get(source)
        source_values = retrieve_pipe.execute()

        # Copy delay promises and insert prediction promises
        move_pipe = self.client.pipeline(transaction=False)
        report = dict()
        report['warnings'] = ''
        execution_report = list()
        for value, destination, method in zip(source_values, destinations, methods):
            if method == 'copy':
                if value is None:
                    report['warnings'] = report['warnings'] + ' None value found '
                else:
                    delay_ttl = int(max(self.DELAYS) + self._DELAY_GRACE + 5 * 60)
                    move_pipe.set(name=destination, value=value, ex=delay_ttl)
                    execution_report.append(
                        {"operation": "set", "destination": destination, "value": value}
                    )
                    report[destination] = str(value)
            elif method == 'predict':
                if len(value):
                    value_as_dict = dict(value)
                    move_pipe.zadd(name=destination, mapping=value_as_dict, ch=True)
                    execution_report.append(
                        {
                            "operation": "zadd",
                            "destination": destination,
                            "len": len(value_as_dict)
                        }
                    )
                    report[destination] = str(len(value_as_dict))
                    owners = [self._scenario_owner(ticket)
                              for ticket in value_as_dict.keys()]
                    unique_owners = list(set(owners))
                    try:
                        move_pipe.sadd(self._OWNERS() + destination, *unique_owners)
                        execution_report.append({
                            "operation": "sadd",
                            "destination": self._OWNERS() + destination,
                            "value": unique_owners
                        })
                    except DataError:
                        report[destination] = "Failed to insert predictions to " + \
                                              destination
            else:
                raise Exception("bug - missing case ")

        execut = move_pipe.execute()
        for record, ex in zip(execution_report, execut):
            record.update({"execution_result": ex})

        return sum(execut) if not with_report else execution_report
