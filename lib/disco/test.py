import os, signal, unittest
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
from SocketServer import ThreadingMixIn
from httplib import OK, INTERNAL_SERVER_ERROR
from threading import Thread

try:
    from unittest import SkipTest
except ImportError:
    class SkipTest(Exception):
        pass

from disco.core import Disco, result_iterator
from disco.job import Job
from disco.ddfs import DDFS
from disco.settings import DiscoSettings
from disco.util import iterify

class InterruptTest(KeyboardInterrupt, SkipTest):
    def __init__(self, test):
        super(InterruptTest, self).__init__("Test interrupted: May not have finished cleaning up")
        self.test = test

    def __call__(self, signum, frame):
        if self.test.is_running:
            self.test.is_running = False
            raise self

class TestCase(unittest.TestCase):
    settings = DiscoSettings()

    @property
    def ddfs(self):
        return DDFS(settings=self.settings)

    @property
    def disco(self):
        return Disco(settings=self.settings)

    @property
    def nodes(self):
        return dict((host, info['max_workers'])
                    for host, info in self.disco.nodeinfo().items()
                    if not info['blacklisted'])

    @property
    def num_workers(self):
        return sum(x['max_workers'] for x in self.disco.nodeinfo().values())

    @property
    def test_server_address(self):
        return (str(self.settings['DISCO_TEST_HOST']),
                int(self.settings['DISCO_TEST_PORT']))

    def assertAllEqual(self, results, answers):
        from disco.future import izip_longest as zip
        for result, answer in zip(results, answers):
            self.assertEquals(result, answer)

    def assertCommErrorCode(self, code, callable):
        from disco.error import CommError
        try:
            ret = callable()
        except CommError, e:
            return self.assertEquals(code, e.code)
        except Exception, e:
            raise AssertionError('CommError not raised, got %s' % e)
        raise AssertionError('CommError not raised (expected %d), '
            'returned %s' % (code, ret))

    def assertResults(self, job, answers):
        self.assertAllEqual(self.results(job), answers)

    def results(self, job, **kwargs):
        return result_iterator(job.wait(), **kwargs)

    def run(self, result=None):
        self.is_running = True
        signal.signal(signal.SIGINT, InterruptTest(self))
        super(TestCase, self).run(result)
        self.is_running = False

    def setUp(self):
        if hasattr(self, 'serve'):
            self.test_server = TestServer.create(self.test_server_address, self.serve)
            self.test_server.start()

    def tearDown(self):
        if hasattr(self, 'serve'):
            self.test_server.stop()
        if hasattr(self, 'job') and self.settings['DISCO_TEST_PURGE']:
            self.job.purge()

    def skipTest(self, message):
        # Workaround for python2.5 which doesn't have skipTest in unittests
        # make sure calls to skipTest are the last statement in a code branch
        # (until we drop 2.5 support)
        try:
            super(TestCase, self).skipTest(message)
        except AttributeError, e:
            pass

class TestJob(Job):
    @property
    def profile(self):
        return bool(self.settings['DISCO_TEST_PROFILE'])

class TestLoader(unittest.TestLoader):
    def __init__(self, settings):
        super(TestLoader, self).__init__()
        self.settings = settings

    def loadTestsFromTestCase(self, testCaseClass):
        if issubclass(testCaseClass, TestCase):
            testCaseClass.settings = self.settings
        return super(TestLoader, self).loadTestsFromTestCase(testCaseClass)

class TestRunner(unittest.TextTestRunner):
    def __init__(self, settings):
        debug_levels = {'off': 0, 'log': 1, 'trace': 2}
        super(TestRunner, self).__init__(verbosity=debug_levels[settings['DISCO_DEBUG']])
        self.settings = settings

    def run(self, *names):
        suite = TestLoader(self.settings).loadTestsFromNames(names)
        return super(TestRunner, self).run(suite)

class TestServer(ThreadingMixIn, HTTPServer):
    allow_reuse_address = True

    @property
    def address(self):
        return 'http://%s:%d' % self.server_address

    @classmethod
    def create(cls, server_address, data_generator):
        return cls(server_address, handler(data_generator))

    def start(self):
        self.thread = Thread(target=self.serve_forever)
        self.thread.start()

    def stop(self):
        # Workaround for Python2.5 which doesn't have shutdown
        if hasattr(self, "shutdown"):
            self.shutdown()
        self.socket.close()

    def urls(self, inputs):
        def serverify(input):
            return '%s/%s' % (self.address, input)
        return [[serverify(url) for url in iterify(input)] for input in inputs]

class FailedReply(Exception):
    pass

def handler(data_generator):
    class Handler(BaseHTTPRequestHandler):
        def send_data(self, data):
            self.send_response(OK)
            self.send_header('Content-length', len(data or []))
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self):
            try:
                self.send_data(data_generator(self.path.strip('/')))
            except FailedReply, e:
                self.send_error(INTERNAL_SERVER_ERROR, str(e))

        def log_request(*args):
            pass # suppress logging output for now
    return Handler
