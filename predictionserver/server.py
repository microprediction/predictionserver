from predictionserver.servermixins.leaderboardserver import LeaderboardServer


class PublishingServer(LeaderboardServer):

    def __init__(self,**kwargs):
        super().__init__(**kwargs)


