import pytest
from predictionserver.app.attributeapp import create_attribute_app
from predictionserver.app.localapp import create_local_app_process, kill_local_app_process


def pytest_collection_modifyitems(items, config):
    """ Adds 'unmarked' to any test not otherwise marked """
    # This allows us to avoid running tests that require local host
    # GitActions will run all unmarked tests
    # https://stackoverflow.com/questions/39846230/how-to-run-only-unmarked-tests-in-pytest
    for item in items:
        if not any(item.iter_markers()):
            item.add_marker("unmarked")


@pytest.fixture(scope="session")
def localhost_process():
    app = create_attribute_app()
    app_process = create_local_app_process(app=app)
    yield app_process
    kill_local_app_process(app_process)

