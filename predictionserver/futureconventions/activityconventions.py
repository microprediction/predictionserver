from predictionserver.futureconventions.typeconventions import StrEnum


class Activity(StrEnum):
    unknown = -1
    other = 0
    submit = 1
    set = 2
    mset = 3
    cset = 4
    put = 5
    touch = 6
    mtouch = 7
    delete = 8
    patch = 9
    notification = 10
    multiply = 11
    cancel = 12
    daemon = 13
    add = 14


class ActivityContext(StrEnum):  # Context in which operation is performed
    unknown = -1
    other = 0
    stream = 1
    cdf = 2
    lagged = 3
    leaderboard = 4
    ratings = 5
    balance = 6
    prize = 7
    announcements = 8
    repository = 9
    messages = 10
    links = 11
    subscription = 12
    transfer = 13
    reward = 14
    bankruptcy = 15
    promise = 16
    attribute = 17
    metric = 18
