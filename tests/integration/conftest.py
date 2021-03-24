import pytest
from predictionserver.app.attributeapp_autogen import create_attribute_app
from predictionserver.app.attributeapp_manual import create_attribute_app_manually

@pytest.fixture
def attribute_client_autogen():
    app = create_attribute_app()
    app.config['TESTING'] = True
    with app.test_client() as client:
        with app.app_context():
            yield client

@pytest.fixture
def attribute_client_manual():
    app = create_attribute_app_manually()
    app.config['TESTING'] = True
    with app.test_client() as client:
        with app.app_context():
            yield client
