from predictionserver.servermixins.attributeserver import AttributeServer
from predictionserver.futureconventions.attributeconventions import AttributeGranularity


class AttributeApi(AttributeServer):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def api_attribute_get(
            self,
            attribute_type: str,
            granularity: str,
            code: str = None,
            write_key: str = None,
            name: str = None,
            delay: int = None,
            genus: str = None,
    ):
        return self.get_attribute(
            attribute_type=attribute_type,
            granularity=granularity,
            code=code,
            write_key=write_key,
            name=name,
            delay=delay,
            genus=genus,
        )

    def api_attribute_put(
            self,
            attribute_type: str,
            value: str,
            granularity: str,
            code: str = None,
            write_key: str = None,
            name: str = None,
            delay: int = None,
            genus: str = None,
    ):
        return self.set_attribute(
            attribute_type=attribute_type,
            value=value,
            granularity=granularity,
            code=code,
            write_key=write_key,
            name=name,
            delay=delay,
            genus=genus,
        )

    def api_attribute_delete(
            self,
            attribute_type: str,
            granularity: str,
            code: str = None,
            write_key: str = None,
            name: str = None,
            delay: int = None,
            genus: str = None,
    ):
        return self.delete_attribute(
            attribute_type=attribute_type,
            granularity=granularity,
            code=code,
            write_key=write_key,
            name=name,
            delay=delay,
            genus=genus,
        )

    def api_attribute_patch(self):
        return "hello world"


class OwnerPublicAttributeApi(AttributeServer):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def api_owner_public_attribute_get(
            self,
            attribute_type: str,
            code: str = None,
    ):
        return self.get_attribute(
            granularity=AttributeGranularity.code,
            attribute_type=attribute_type,
            code=code,
        )

    def api_owner_public_attribute_put(
            self,
            attribute_type: str,
            write_key: str,
            value: str,
    ):
        return self.set_attribute(
            granularity=AttributeGranularity.code,
            attribute_type=attribute_type,
            write_key=write_key,
            code=self.shash(write_key),
            value=value,
            verbose=True,
        )

    def api_owner_public_attribute_delete(
            self,
            attribute_type: str,
            write_key: str,
    ):
        return self.delete_attribute(
            granularity=AttributeGranularity.code,
            attribute_type=attribute_type,
            write_key=write_key,
            code=self.shash(write_key),
            verbose=True,
        )

    def api_owner_public_attribute_patch(self):
        return "hello world"


class OwnerPrivateAttributeApi(AttributeServer):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def api_owner_private_attribute_get(
            self,
            attribute_type: str,
            write_key: str = None,
    ):
        return self.get_attribute(
            granularity=AttributeGranularity.write_key,
            attribute_type=attribute_type,
            write_key=write_key,
        )

    def api_owner_private_attribute_put(
            self,
            attribute_type: str,
            write_key: str,
            value: str,
    ):
        return self.set_attribute(
            granularity=AttributeGranularity.write_key,
            attribute_type=attribute_type,
            write_key=write_key,
            value=value,
            verbose=True,
        )

    def api_owner_private_attribute_delete(
            self,
            attribute_type: str,
            write_key: str,
    ):
        return self.delete_attribute(
            granularity=AttributeGranularity.write_key,
            attribute_type=attribute_type,
            write_key=write_key,
            verbose=True,
        )

    def api_owner_public_attribute_patch(self):
        return "hello world again"


class StreamAttributeApi(AttributeServer):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def api_stream_attribute_get(
            self,
            attribute_type: str,
            name: str,
    ):
        return self.get_attribute(
            granularity=AttributeGranularity.name,
            attribute_type=attribute_type,
            name=name,
        )

    def api_stream_attribute_put(
            self,
            attribute_type: str,
            write_key: str,
            name: str,
            value: str,
    ):
        return self.set_attribute(
            granularity=AttributeGranularity.name,
            attribute_type=attribute_type,
            write_key=write_key,
            name=name,
            value=value,
            verbose=True,
        )

    def api_stream_attribute_delete(
            self,
            attribute_type: str,
            write_key: str,
            name: str
    ):
        return self.delete_attribute(
            granularity=AttributeGranularity.name,
            attribute_type=attribute_type,
            write_key=write_key,
            name=name,
            verbose=True
        )

    def api_owner_public_attribute_patch(self):
        return "hello world again"
