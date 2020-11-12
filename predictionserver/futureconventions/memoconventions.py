from predictionserver.futureconventions.typeconventions import StrEnum, Genus, Memory, GranularityEnum
from predictionserver.futureconventions.attributeconventions import AttributeType
from predictionserver.futureconventions.metricconventions import MetricType
from predictionserver.futureconventions.hashconventions import HashType
from predictionserver.futureconventions.sepconventions import SepConventions
from predictionserver.futureconventions.activityconventions import Activity, ActivityContext
from predictionserver.futureconventions.sortedsetconventions import SortedSetType
from collections import OrderedDict
import datetime
import time
from copy import deepcopy
import uuid


# A memo enforces some consistency on messages used internally and in confirms, warnings, errors
# and responses to put style requests.


class PublicActor(StrEnum):
    announcer = 0     # Short major announcements shown prominently in dashboard


class MemoCategory(StrEnum):
    alert = 0
    confirm = 1
    warning = 2
    error = 3
    transaction = 4


class MemoGranularity(GranularityEnum):
    public_actor = 0     # Public messages
    private_actor = 1    # System messages from daemons, internal errors etc
    write_key = 2        # User confirms, transactions, warnings, errors and alerts
    name_and_delay = 3   # e.g. Transactions for a stream


class Memo(OrderedDict):
    _ENUM_FIELDS = ['activity', 'public_actor','private_actor','context','attribute',
                    'category', 'granularity','genus','hash_type','sortedset_type',
                    'memory']
    _SANITIZE_FIELDS = {'write_key':'code','counterparty':'counterparty_code'}

    def __init__(self,
                 memo_id:str=None,
                 activity: Activity = None,
                 public_actor: PublicActor=None,
                 private_actor: str=None,
                 context: ActivityContext = None,
                 attribute: AttributeType = None,
                 metric: MetricType = None,
                 hash_type: HashType = None,
                 sortedset_type: SortedSetType = None,
                 granularity: GranularityEnum = None,
                 genus: Genus = None,
                 memory: Memory = None,
                 epoch_time: float = None,
                 timestr: str = None,
                 write_key: str = None,  # Omitted from log
                 code:str=None,  # Will be clobbered by shash(write_key) when log is written
                 counterparty_write_key:str=None,  # Omitted from log
                 counterparty_code: str = None,  # Will be clobbered by shash(counterparty) when log is written
                 name: str = None,
                 index: int = None,
                 delay: int = None,
                 value=None,
                 low=None,  # Transaction results ...
                 high=None,
                 count=None,
                 near=None,
                 avg_near=None,
                 url: str = None,
                 success: int = 1,
                 execution: int = -1,
                 allowed: int = None,
                 message: str = None,
                 text: str = None,
                 host: str = None,
                 data: dict = None
                 ):
        self._initialized = False
        super().__init__(memo_id=memo_id, activity=activity, public_actor=public_actor,
                         private_actor=private_actor, context=context,
                         attribute=attribute, metric=metric, hash_type=hash_type, sortedset_type=sortedset_type,
                         granularity=granularity, genus=genus, memory=memory, epoch=epoch_time, timestr=timestr,
                         write_key=write_key, counterparty=counterparty_write_key,
                         counterparty_code=counterparty_code, name=name, value=value, delay=delay,
                         success=success, code=code, execution=execution, url=url,
                         text=text, index=index, count=count, allowed=allowed,
                         message=message, host=host, data=data, low=low, high=high,
                         near=near, avg_near=avg_near)
        if self.get('epoch_time') is None:
            self['epoch_time'] = time.time()
        if self.get('timestr') is None:
            self['timestr'] = str(datetime.datetime.now())
        if self.get('memo_id') is None:
            self['memo_id'] = str(uuid.uuid4())
        self._initialized = True

    def __setitem__(self, key, value):
        if self._initialized:
            if key not in self.keys():
                raise Exception(key + ' is not a valid memo field')
            else:
                existing_value = self[key]
                if existing_value is None or type(value) == type(existing_value):
                    super().__setitem__(key, value)
                else:
                    raise Exception(key + ' is supposed to be type ' + str(type(existing_value)))
        else:
            super().__setitem__(key, value)

    def as_dict(self, cast_to_str=True, leave_out_none=True, flatten_data=True):
        d = OrderedDict([(k, v) for k, v in dict(self).items() if v is not None]) if leave_out_none else OrderedDict(
            self)
        if cast_to_str:
            for k in self._ENUM_FIELDS:
                if k in d:
                    d[k] = str(d[k])
        if flatten_data and d.get('data') is not None:
            if isinstance(d.get('data'), dict):
                data = deepcopy(d['data'])
                del (d['data'])
                d.update(data)
                return d
        return d


class MemoConventions:

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
