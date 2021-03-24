from flask import Flask
from predictionserver.api.attributeapi import OwnerPrivateAttributeApi,\
    OwnerPublicAttributeApi, StreamAttributeApi, AttributeApi
from flask_restx import Namespace, Api
from predictionserver.app.apimaker import restx_class_maker
from predictionserver.futureconventions.apiconventions import ApiMethod
from predictionserver.futureconventions.attributeconventions import AttributeGranularity,\
    ATTRIBUTE_GRANULARITY_EXPLANATIONS


def create_attribute_app():
    """ Creates a flask application with attribute APIs """
    app = Flask(__name__)
    api = Api(app)
    add_attribute_namespaces(api=api)
    return app


def add_attribute_namespaces(api: Api):
    """ Adds attribute APIs to a flask_restx application """
    api_methods = [
        ApiMethod.get, ApiMethod.put, ApiMethod.delete, ApiMethod.patch
    ]
    objs = [
        OwnerPrivateAttributeApi(),
        OwnerPublicAttributeApi(),
        StreamAttributeApi(),
        AttributeApi(),
    ]
    granularities = [
        AttributeGranularity.write_key,
        AttributeGranularity.code,
        AttributeGranularity.name,
    ]
    docstrings = (
        [ATTRIBUTE_GRANULARITY_EXPLANATIONS[g] for g in granularities] +
        [' any attribute ']
    )
    api_classes = [
        restx_class_maker(api_obj=obj, docstring=docstring, api_methods=api_methods)
        for obj, docstring in zip(objs, docstrings)
    ]

    for api_class in api_classes:
        ns = Namespace(api_class.__name__)
        api.add_namespace(ns=ns)
        ns.add_resource(api_class, '/')
