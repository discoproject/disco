import os, signal
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
from SocketServer import ThreadingMixIn
from httplib import OK, INTERNAL_SERVER_ERROR
from threading import Thread
from unittest import TestCase, TestLoader, TextTestRunner

try:
    from unittest import SkipTest
except ImportError:
    class SkipTest(Exception):
        pass

import disco
from disco.core import Disco, result_iterator
from disco.settings import DiscoSettings
from disco.util import rapply

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
        # Workaround for Python2.5 which doesn't have the shutdown
        # method
        if hasattr(self, "shutdown"):
            self.shutdown()
        self.socket.close()

    def urls(self, inputs):
        def serverify(input):
            return '%s/%s' % (self.address, input)
        return rapply(inputs, serverify)

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

class InterruptTest(KeyboardInterrupt, SkipTest):
    def __init__(self, test):
        super(InterruptTest, self).__init__("Test interrupted: May not have finished cleaning up")
        self.test = test

    def __call__(self, signum, frame):
        if self.test.is_running:
            self.test.is_running = False
            raise self

class DiscoTestCase(TestCase):
    disco_settings = DiscoSettings()

    @property
    def disco_master_url(self):
        return self.disco_settings['DISCO_MASTER']

    @property
    def disco(self):
        return Disco(self.disco_master_url)

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

    def run(self, result=None):
        self.is_running = True
        signal.signal(signal.SIGINT, InterruptTest(self))
        super(DiscoTestCase, self).run(result)
        self.is_running = False

class DiscoJobTestFixture(object):
    jobargs = ('input',
               'combiner',
               'map',
               'map_init',
               'map_input_stream',
               'map_output_stream',
               'map_reader',
               'map_writer',
               'mem_sort_limit',
               'merge_partitions',
               'params',
               'partition',
               'partitions',
               'profile',
               'save',
               'scheduler',
               'sort',
               'reduce',
               'reduce_init',
               'reduce_input_stream',
               'reduce_output_stream',
               'reduce_reader',
               'reduce_writer',
               'required_files',
               'required_modules',
               'ext_params',
               'ext_map',
               'ext_reduce')
    result_reader = staticmethod(disco.func.chain_reader)

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
        return (str(self.disco_settings['DISCO_TEST_HOST']),
                int(self.disco_settings['DISCO_TEST_PORT']))

    @property
    def profile(self):
        return bool(self.disco_settings['DISCO_TEST_PROFILE'])

    @property
    def results(self):
        return result_iterator(self.job.wait(), reader=self.result_reader)

    @property
    def input(self):
        return self.test_server.urls(self.inputs)

    def getdata(self, path):
        pass

    def setUp(self):
        self.test_server = TestServer.create(self.test_server_address, self.getdata)
        self.test_server.start()
        try:
            jobargs = {'name': self.__class__.__name__}
            for jobarg in self.jobargs:
                if hasattr(self, jobarg):
                    jobargs[jobarg] = getattr(self, jobarg)

            self.job = self.disco.new_job(**jobargs)
        except:
            self.test_server.stop()
            raise

    def tearDown(self):
        self.test_server.stop()
        if self.disco_settings['DISCO_TEST_PURGE']:
            self.job.purge()

    def runTest(self):
        from disco.future import izip_longest as zip
        for result, answer in zip(self.results, self.answers):
            self.assertEquals(result, answer)

    def skipTest(self, message):
        # Workaround for python2.5 which doesn't have skipTest in unittests
        # make sure calls to skipTest are the last statement in a code branch
        # (until we drop 2.5 support)
        try:
            super(DiscoJobTestFixture, self).skipTest(message)
        except AttributeError, e:
            pass

class DiscoMultiJobTestFixture(DiscoJobTestFixture):
    def result_reader(self, m):
        return disco.func.netstr_reader

    def profile(self, m):
        return bool(self.disco_settings['DISCO_TEST_PROFILE'])

    def results(self, m):
        return result_iterator(self.jobs[m].wait(),
                       reader=getattr(self, 'result_reader_%d' % (m + 1)))

    def input(self, m):
        return self.test_servers[m].urls(getattr(self, 'inputs_%d' % (m + 1)))

    def __getattribute__(self, name):
        try:
            return super(DiscoMultiJobTestFixture, self).__getattribute__(name)
        except AttributeError:
            for prefix in ('input', 'profile', 'results', 'result_reader'):
                if name.startswith('%s_' % prefix):
                    attribute, n = name.rsplit('_', 1)
                    return getattr(self, attribute)(int(n) - 1)
            raise

    def setUp(self):
        host, port    = self.test_server_address
        self.test_servers = [None] * self.njobs
        self.jobs     = [None] * self.njobs
        for m in xrange(self.njobs):
            n = m + 1
            self.test_servers[m] = TestServer.create((host, port + m),
                                 getattr(self, 'getdata_%d' % n, self.getdata))
            self.test_servers[m].start()
            try:
                jobargs = {'name': '%s_%d' % (self.__class__.__name__, n)}
                for jobarg in self.jobargs:
                    attr = getattr(self, '%s_%d' % (jobarg, n), None)
                    if attr:
                        jobargs[jobarg] = attr

                self.jobs[m] = self.disco.new_job(**jobargs)
                setattr(self, 'job_%d' % n, self.jobs[m])
            except:
                for k in xrange(n):
                    self.test_servers[k].stop()
                raise

    def tearDown(self):
        for m in xrange(self.njobs):
            self.test_servers[m].stop()
            if self.disco_settings['DISCO_TEST_PURGE']:
                self.jobs[m].purge()

    def runTest(self):
        for m in xrange(self.njobs):
            n = m + 1
            for result, answer in zip(getattr(self, 'results_%d' % n),
                                      getattr(self, 'answers_%d' % n)):
                self.assertEquals(result, answer)

class DiscoTestLoader(TestLoader):
    def __init__(self, disco_settings):
        super(DiscoTestLoader, self).__init__()
        self.disco_settings = disco_settings

    def loadTestsFromTestCase(self, testCaseClass):
        if issubclass(testCaseClass, DiscoTestCase):
            testCaseClass.disco_settings = self.disco_settings
        return super(DiscoTestLoader, self).loadTestsFromTestCase(testCaseClass)

class DiscoTestRunner(TextTestRunner):
    def __init__(self, disco_settings):
        debug_levels = {'off': 0, 'log': 1, 'trace': 2}
        super(DiscoTestRunner, self).__init__(verbosity=debug_levels[disco_settings['DISCO_DEBUG']])
        self.disco_settings = disco_settings

    def run(self, *names):
        suite = DiscoTestLoader(self.disco_settings).loadTestsFromNames(names)
        return super(DiscoTestRunner, self).run(suite)
