import pytest
from predictionserver.app.attributeapp import create_attribute_app
from predictionserver.app.localapp import create_local_app_process, kill_local_app_process


@pytest.fixture
def attribute_client():
    app = create_attribute_app()
    process = create_local_app_process(app=app)
    yield app.test_client()
    kill_local_app_process(process=process)

