from predictionserver.futureconventions.typeconventions import GranularityEnum, StrEnum


class SortedSetType(StrEnum):
    leaderboard = 0
    samples = 1
    predictions = 2


class SortedSetNameGranularity(GranularityEnum):
    memory = 0
    code = 1
    code_and_genus = 3
    name = 3
    name_and_delay = 4


class SortedSetKeyGranularity(GranularityEnum):
    other = -1                   # Hack for storing anything
    code = 0
    index_and_write_key = 1      # Stores supplied scenarios


class SortedSetConventions:

    def __init__(self, **kwargs):
        super().__init__(*kwargs)
