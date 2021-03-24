from copy import deepcopy
from flask import Flask
from flask_cors import CORS
from flask_restx import Resource, Api, Namespace, reqparse

from predictionserver.api.attributeapi import OwnerPublicAttributeApi, OwnerPrivateAttributeApi, StreamAttributeApi
from predictionserver.app.apimaker import make_parser
from predictionserver.futureconventions.apiconventions import ApiMethod
from predictionserver.futureconventions.attributeconventions import AttributeGranularity
from predictionserver.serverhabits.attributehabits import attribute_docstring
from predictionserver.set_config import MICRO_TEST_CONFIG

import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


CONNECTION = deepcopy(MICRO_TEST_CONFIG)


    #------------------- #
    #  Attribute APIs    #
    #------------------- #


def create_attribute_app_manually():
    """ Creates a flask application with attribute APIs """
    app = Flask(__name__)
    api = Api(app)
    add_attribute_namespaces_manually(api=api)
    return app


def add_attribute_namespaces_manually(api):

    owner_public_attribute_api = OwnerPublicAttributeApi()
    owner_public_attribute_parsers = dict( [ ( str(api_method), make_parser(obj=owner_public_attribute_api, api_method=api_method ))
                                             for api_method in [ApiMethod.get, ApiMethod.put, ApiMethod.delete] ] )
    # owner_public_attribute_get_parser = reqparse.RequestParser()
    # owner_public_attribute_get_parser.add_argument('code',required=True)

    class OwnerPublicAttribute(Resource):

        @attribute_docstring(granularity=AttributeGranularity.code)
        def get(self):
            kwargs = owner_public_attribute_parsers[str(ApiMethod.get)].parse_args()
            # kwargs = owner_public_attribute_get_parser.parse_args()  # Override
            owner_public_attribute_api.connect(**CONNECTION)
            return owner_public_attribute_api.api_owner_public_attribute_get(**kwargs)

        @attribute_docstring(granularity=AttributeGranularity.code)
        def put(self):
            kwargs = owner_public_attribute_parsers[str(ApiMethod.put)].parse_args()
            owner_public_attribute_api.connect(**CONNECTION)
            return owner_public_attribute_api.api_owner_public_attribute_put(**kwargs)

        @attribute_docstring(granularity=AttributeGranularity.code)
        def delete(self):
            kwargs = owner_public_attribute_parsers[str(ApiMethod.delete)].parse_args()
            owner_public_attribute_api.connect(**CONNECTION)
            return owner_public_attribute_api.api_owner_public_attribute_delete(**kwargs)


    owner_public_attribute_ns = Namespace('public_attribute',description='Get or modify public owner attribute')
    api.add_namespace(ns=owner_public_attribute_ns)
    owner_public_attribute_ns.add_resource(OwnerPublicAttribute,'/')

    # -+-

    owner_private_attribute_api = OwnerPrivateAttributeApi()
    owner_private_attribute_parsers = dict( [ ( str(api_method), make_parser(obj=owner_private_attribute_api, api_method=api_method ))
                                              for api_method in [ApiMethod.get, ApiMethod.put,ApiMethod.delete] ] )

    class OwnerPrivateAttribute(Resource):

        @attribute_docstring(granularity=AttributeGranularity.write_key)
        def get(self):
            kwargs = owner_private_attribute_parsers[str(ApiMethod.get)].parse_args()
            owner_private_attribute_api.connect(**CONNECTION)
            return owner_private_attribute_api.api_owner_private_attribute_get(**kwargs)

        @attribute_docstring(granularity=AttributeGranularity.write_key)
        def put(self):
            kwargs = owner_private_attribute_parsers[str(ApiMethod.put)].parse_args()
            owner_private_attribute_api.connect(**CONNECTION)
            return owner_private_attribute_api.api_owner_private_attribute_get(**kwargs)

        @attribute_docstring(granularity=AttributeGranularity.write_key)
        def delete(self):
            kwargs = owner_private_attribute_parsers[str(ApiMethod.delete)].parse_args()
            owner_private_attribute_api.connect(**CONNECTION)
            return owner_private_attribute_api.api_owner_private_attribute_delete(**kwargs)

    owner_private_attribute_ns = Namespace('private_attribute',description='Get or modify private owner attribute')
    api.add_namespace(ns=owner_private_attribute_ns)
    owner_private_attribute_ns.add_resource(OwnerPrivateAttribute,'/')

    # -+-

    stream_attribute_api = StreamAttributeApi()
    stream_attribute_parsers = dict( [ ( str(api_method), make_parser(obj=stream_attribute_api,
                                            api_method=api_method )) for api_method in [ApiMethod.get,ApiMethod.put, ApiMethod.delete] ] )

    class StreamAttribute(Resource):

        @attribute_docstring(granularity=AttributeGranularity.name)
        def get(self):
            kwargs = stream_attribute_parsers[str(ApiMethod.get)].parse_args()
            stream_attribute_api.connect(**CONNECTION)
            return stream_attribute_api.api_stream_attribute_get(**kwargs)

        @attribute_docstring(granularity=AttributeGranularity.name)
        def put(self):
            kwargs = stream_attribute_parsers[str(ApiMethod.put)].parse_args()
            stream_attribute_api.connect(**CONNECTION)
            return stream_attribute_api.api_stream_attribute_get(**kwargs)

        @attribute_docstring(granularity=AttributeGranularity.name)
        def delete(self):
            kwargs = stream_attribute_parsers[str(ApiMethod.delete)].parse_args()
            stream_attribute_api.connect(**CONNECTION)
            return stream_attribute_api.api_stream_attribute_delete(**kwargs)


    stream_attribute_ns = Namespace('stream_attribute',description='Get or modify public stream attribute')
    api.add_namespace(ns=stream_attribute_ns)
    stream_attribute_ns.add_resource(StreamAttribute,'/')

    return api