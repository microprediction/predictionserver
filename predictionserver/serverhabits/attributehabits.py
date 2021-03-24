from predictionserver.futureconventions.attributeconventions import (
    AttributeConventions,
    AttributeType,
    AttributeGranularity,
    ATTRIBUTE_GRANULARITY_EXPLANATIONS,
)
from predictionserver.futureconventions.keyconventions import KeyConventions
from predictionserver.futureconventions.sepconventions import SepConventions
from predictionserver.serverhabits.obscurityhabits import ObscurityHabits
from predictionserver.futureconventions.apiconventions import ApiMethod
from typing import Union

# Example hash table name         Example keys       Example values     Explanation
# -----------------------------------------------------------------------------------------
# attribute::email::write_key     'lkajsf124124jlk'   bob@bill.com   (privately held email)
# attribute::homepage::code       'f10a7ab1eca7'      https://blah   (public homepage link)


def attribute_docstring(granularity: AttributeGranularity):
    """ Decorator that standardized the doc strings for API """

    def _doc(method):
        api_method = ApiMethod[method.__name__]
        method.__doc__ = str(api_method) + ' ' + \
            ATTRIBUTE_GRANULARITY_EXPLANATIONS[granularity]
        return method

    return _doc


class AttributeHabits(AttributeConventions, ObscurityHabits):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def attribute_location(
            self,
            attribute: Union[AttributeType, str],
            granularity: Union[AttributeGranularity, str]
    ):
        """
        Determine attribute hash table name, such as  attributes::email::write_key
        """
        attribute = AttributeType[str(attribute)]
        granularity = AttributeGranularity[str(granularity)]
        return self.obscurity() + 'attributes' + SepConventions.sep() + \
            str(attribute) + SepConventions.sep() + str(granularity)

    def attribute_key(self,
                      granularity: Union[AttributeGranularity, str] = None,
                      write_key=None,
                      code=None,
                      name=None,
                      delay=None,
                      genus=None):
        """ Determine attribute key, such as   'f10a7ab1e|310  """
        # It is strongly recommended that one supplies granularity,
        # though it can be implied
        granularity = AttributeGranularity[str(granularity)] if granularity else \
            self.attribute_granularity_inference(
                write_key=write_key, code=code, name=name, genus=genus, delay=delay
        )
        if 'code' in str(granularity):
            if code is not None:
                code = KeyConventions.code_from_code_or_key(code_or_key=code)
            elif write_key is not None:
                code = KeyConventions.shash(write_key)
            else:
                raise TypeError(
                    'Must supply code (or write_key) if it is part of attribute '
                    'granularity'
                )

        return granularity.instance_name(
            write_key=write_key,
            code=code,
            name=name,
            genus=genus,
            delay=delay)

    def attribute_location_and_key(
            self,
            attribute: AttributeType,
            granularity: AttributeGranularity,
            **kwargs
    ):
        """ How attributes are stored in redis """
        location = self.attribute_location(attribute=attribute, granularity=granularity)
        key = self.attribute_key(granularity=granularity, **kwargs)
        return location, key

    def attribute_location_by_inference(
            self, attribute: Union[AttributeType, str], **kwargs):
        """ Implicitly determine attribute hash table name """
        granularity = self.attribute_granularity_inference(**kwargs)
        return self.attribute_location(attribute=attribute, granularity=granularity)
