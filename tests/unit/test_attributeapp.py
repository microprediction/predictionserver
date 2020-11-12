from flask import Flask
from flask_restx import Api
from predictionserver.app.attributeapp_autogen import add_attribute_namespaces


def test_attribute_namespaces():
    app = Flask(__name__)
    api = Api(app)
    api = add_attribute_namespaces(api=api)
    namespace_names = [ ns.name for ns in api.namespaces ]
    assert 'OwnerPrivateAttribute' in namespace_names