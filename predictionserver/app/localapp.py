# For testing
import multiprocessing
import time
from urllib.request import urlopen
from urllib.error import URLError
import os
import logging
import signal


def create_local_app_process(app):
    """ Run localhost flask app in separate process """

    host = 'localhost'
    port = 5000
    url = 'http://%s:%d' % (host, port)
    app.config['SERVERNAME'] = host + ':' + str(port)

    def worker(app, host, port):
        app.run(host=host, port=port, use_reloader=False, threaded=True)

    process = multiprocessing.Process(
        target=worker,
        args=(app, host, port)
    )
    process.start()

    # We must wait for the server to start listening with a maximum
    # timeout of 5 seconds.
    timeout = 5
    while timeout > 0:
        time.sleep(1)
        try:
            urlopen(url)
            timeout = 0
        except URLError:
            timeout -= 1
    return process


def kill_local_app_process(process, timeout=5):

    def nice_kill(prcss):
        try:
            os.kill(prcss.pid, signal.SIGINT)
            prcss.join(timeout)
            return True
        except Exception as ex:
            logging.error('Failed to join the live server process: %r', ex)
            return False

    def ugly_kill(prcss):
        if prcss.is_alive():
            prcss.terminate()

    if nice_kill(process):
        return
    ugly_kill(process)

