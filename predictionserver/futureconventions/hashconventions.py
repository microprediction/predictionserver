from predictionserver.futureconventions.typeconventions import GranularityEnum, StrEnum

# Listing of tables and granularities that would typically be relevant...


class HashType(StrEnum):           # Usual granularity used ...
    performance = 1                # write_key
    links = 2
    priority = 3


class HashNameGranularity(GranularityEnum):
    other = -1
    write_key = 0
    code = 1
    name = 2
    name_and_delay = 3


class HashKeyGranularity(GranularityEnum):
    write_key = 0
    code = 1
    name = 2
    name_and_delay = 3   # Used for performance


class HashConventions:

    def __init__(self,**kwargs):
        super().__init__(**kwargs)


