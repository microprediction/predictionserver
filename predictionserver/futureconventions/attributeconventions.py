from predictionserver.futureconventions.typeconventions import (
    GranularityEnum, StrEnum
)

# Listing of attributes and granularities that would typically be relevant...


class AttributeType(StrEnum):      # Usual granularity used ...
    repository = 1                 # code
    email = 2
    # write_key
    description = 3                # name, code
    article = 4                    # name, code
    linkedin = 5                   # name, code
    homepage = 6                   # name, code
    paper = 7                      # name, code
    topic = 8                      # name, code
    update = 9                     # write_key
    team = 10
    handle = 11


class AttributeGranularity(GranularityEnum):
    code = 0                         # Use this for public owner attributes
    write_key = 1                    # Use this for private owner attributes
    name = 2                         # Stream attributes
    name_and_delay = 3               # Horizon attributes
    code_and_genus = 4               # Prize descriptions


LIKELY_ATTRIBUTES = {
    AttributeGranularity.code: [
        AttributeType.repository,
        AttributeType.handle,
        AttributeType.description,
        AttributeType.homepage,
        AttributeType.team,
        AttributeType.article,
    ],
    AttributeGranularity.write_key: [
        AttributeType.email
    ],
    AttributeGranularity.name: [
        AttributeType.repository,
        AttributeType.description,
        AttributeType.homepage,
        AttributeType.article,
        AttributeType.topic,
    ],
}


def _phrase(attributes: list):
    if len(attributes) == 1:
        return str(attributes[0])
    else:
        return ', '.join([str(att) for att in attributes[:-1]]) + ' or ' + \
               str(attributes[-1])


ATTRIBUTE_GRANULARITY_EXPLANATIONS = {
    AttributeGranularity.code: 'public owner attribute such as ' + _phrase(
        LIKELY_ATTRIBUTES[AttributeGranularity.code]
    ),
    AttributeGranularity.write_key: 'private owner attribute such as ' + _phrase(
        LIKELY_ATTRIBUTES[AttributeGranularity.write_key]
    ),
    AttributeGranularity.name: 'public stream attribute such as ' + _phrase(
        LIKELY_ATTRIBUTES[AttributeGranularity.name]
    ),
}


class AttributeConventions:

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def attribute_granularity_inference(
            code=None,
            write_key=None,
            name=None,
            delay=None,
            genus=None
    ):
        """ This is here so the user doesn't need to pass granularity, though
            it is recommended that one does anyway.
        """
        if genus is not None and code is not None:
            return AttributeGranularity.code_and_genus
        elif code is not None:
            return AttributeGranularity.code
        elif delay is not None:
            return AttributeGranularity.name_and_delay
        elif name is not None:
            return AttributeGranularity.name
        elif write_key is not None:
            return AttributeGranularity.write_key
        else:
            raise TypeError(
                'Attribute granularity cannot be inferred. '
                'Suggest supplying granularity explicitly.'
            )
