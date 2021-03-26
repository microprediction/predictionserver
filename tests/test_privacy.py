import pytest
from predictionserver.devtools.whereami import devlocation

# A little check on .gitignore


def test_privacy():
    if devlocation() == 'github':
        verify_private_file_not_on_github()
    else:
        verify_private_file_exists_locally()


def verify_private_file_exists_locally():
    from predictionserver.private.supposed_to_be_private import should_only_run_locally
    assert should_only_run_locally()


def verify_private_file_not_on_github():
    with pytest.raises(ImportError):
        from predictionserver.private.supposed_to_be_private import should_only_run_locally
        should_only_run_locally()
