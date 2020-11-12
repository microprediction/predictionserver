import os
from sys import platform
from getjson import getjson

# Load a couple of environment variables for local testing.
# These are secrets on github.
# Locally you should have a .private.set_env_private that creates the environment variables, or set some other way
try:
    from predictionserver.private.set_env_private import NOTHING_MUCH
except:
    pass

micro_config_url = os.getenv('TEST_CONFIG_URL')
micro_config_failover_url = os.getenv('TEST_CONFIG_FAILOVER_URL')
MICRO_TEST_CONFIG = getjson(url=micro_config_url, failover_url=micro_config_failover_url)

if MICRO_TEST_CONFIG is None:
    raise Exception('Could not get configuration')

if platform == 'darwin':
    MICRO_TEST_CONFIG['DUMP'] = True

MICRO_CONVENTIONS_ARGS = ('num_predictions', 'min_len', 'min_balance', 'delays')

SERVER_FAKE_CONFIG = MICRO_TEST_CONFIG['FAKE']
for arg in MICRO_CONVENTIONS_ARGS:
    if arg not in SERVER_FAKE_CONFIG:
        raise Exception('Need to update SERVER_FAKE_CONFIG to include ' + arg)

TESTING_KEYS = [kn[0] for kn in MICRO_TEST_CONFIG['TESTING_KEYS']]
