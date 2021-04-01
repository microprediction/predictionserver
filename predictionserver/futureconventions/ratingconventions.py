from predictionserver.futureconventions.sepconventions import SepConventions
from predictionserver.futureconventions.typeconventions import GranularityEnum


class RatingGranularity(GranularityEnum):
    """ Enumerates varieties of ratings """
    code = 0  # Participant strength
    name_and_delay = 1  # Horizon strength


class RatingConventions:

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.RATING = "rating" + SepConventions.sep()

    def rating_name(self, granularity):
        """ Name for rating tables """
        return self.RATING + str(granularity) + SepConventions.sep()

    @staticmethod
    def rating_key(granularity: RatingGranularity, **kwargs):
        """ How ratings are listed in the rating table """
        return granularity.instance_name(**kwargs)


if __name__ == '__main__':
    sc = RatingConventions()
    for granularity in RatingGranularity:
        print(granularity)
        print(
            sc.rating_key(
                granularity=granularity,
                name='bill',
                sponsor='mary',
                memory=10000,
                delay=72,
                host='home',
                genus='bivariate',
                code='notacode'
            )
        )
