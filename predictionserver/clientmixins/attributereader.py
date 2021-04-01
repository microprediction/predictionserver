from predictionserver.futureconventions.attributeconventions import (
    AttributeType, AttributeGranularity
)
from predictionserver.clientmixins.basereader import BaseReader
from typing import Union


class AttributeReader(BaseReader):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def get_attribute(
            self,
            attribute_type: AttributeType,
            granularity: Union[AttributeGranularity, str],
            **kwargs,
    ) -> Union[str, int, float]:
        """
            Supply all arguments implied by the AttributeGranularity, for example
            name and delay. Or see special cases below.
        """
        granularity = AttributeGranularity[str(granularity)] if granularity \
            else self.attribute_granularity_inference(**kwargs)
        kwargs.update({"granularity": str(granularity)})
        return self.request_get_json(
            method='attribute',
            arg=str(attribute_type),
            data=kwargs,
        )

    # ----------------------------------------------- #
    #    Special cases that are backward compatible   #
    # ----------------------------------------------- #

    def get_owner_repository(self, code: str) -> str:
        new_repo = self.get_attribute(
            attribute_type=AttributeType.repository,
            granularity=AttributeGranularity.code,
            code=code,
        )
        old_repo = self.get_old_owner_repository(code=code)
        return new_repo or old_repo

    def get_owner_email(self, write_key: str) -> str:
        return self.get_attribute(
            attribute_type=AttributeType.email,
            granularity=AttributeGranularity.write_key,
            write_key=write_key,
        )

    def get_stream_description(self, name: str) -> str:
        return self.get_attribute(
            attribute_type=AttributeType.description,
            granularity=AttributeGranularity.name,
            name=name,
        )

    # --------------------------- #
    #    Backward compatibility   #
    # --------------------------- #

    def get_old_owner_repository(self, code: str) -> str:
        return self.request_get_json(method='repository', arg=code)


if __name__ == '__main__':
    from predictionserver.collider_config_private import ALBAHACA_MOLE
    from predictionserver.futureconventions.keyconventions import KeyConventions
    client = AttributeReader()
    code = KeyConventions.shash(write_key=ALBAHACA_MOLE)
    print(client.get_owner_repository(code=code))
