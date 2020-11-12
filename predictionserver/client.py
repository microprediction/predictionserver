from predictionserver.clientmixins.attributereader import AttributeReader




class PublishingServer(LeaderboardServer):

    def __init__(self,**kwargs):
        super().__init__(**kwargs)


