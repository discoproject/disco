import os
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
from SocketServer import ThreadingMixIn
from httplib import OK, INTERNAL_SERVER_ERROR
from threading import Thread
from unittest import TestCase, TestLoader, TextTestRunner

import disco
from disco.core import Disco, result_iterator
from disco.settings import DiscoSettings

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
                self.shutdown()
                self.socket.close()

        def urls(self, inputs):
                return ['%s/%s' % (self.address, input) for input in inputs]

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
                                self.send_error(INTERNAL_SERVER_ERROR, e.message)

                def log_request(*args):
                        pass # suppress logging output for now

        return Handler

class DiscoTestCase(TestCase):
        disco_settings = DiscoSettings()

class DiscoJobTestFixture(object):
        jobargs = ('input',
                   'map',
                   'map_input_stream',
                   'map_output_stream',
                   'map_reader',
                   'map_writer',
                   'mem_sort_limit',
                   'params',
                   'partition',
                   'profile',
                   'scheduler',
                   'sort',
                   'reduce',
                   'reduce_input_stream',
                   'reduce_output_stream',
                   'reduce_reader',
                   'reduce_writer',
                   'nr_maps',
                   'nr_reduces')
        result_reader = staticmethod(disco.func.netstr_reader)

        @property
        def num_workers(self):
                return sum(x['max_workers'] for x in self.disco.nodeinfo()['available'])

        @property
        def disco_master_url(self):
                return 'disco://%s' % self.disco_settings['DISCO_MASTER_HOST']

        @property
        def test_server_address(self):
                return (str(self.disco_settings['DISCO_TEST_HOST']),
                        int(self.disco_settings['DISCO_TEST_PORT']))

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
                        self.disco = Disco(self.disco_master_url)
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
                self.job.purge()

        def runTest(self):
                for result, answer in zip(self.results, self.answers):
                        self.assertEquals(result, answer)

class DiscoMultiJobTestFixture(DiscoJobTestFixture):
        def result_reader(self, m):
                return disco.func.netstr_reader

        def results(self, m):
                return result_iterator(self.jobs[m].wait(),
                                       reader=getattr(self, 'result_reader_%d' % (m + 1)))

        def input(self, m):
                return self.test_servers[m].urls(getattr(self, 'inputs_%d' % (m + 1)))

        def __getattribute__(self, name):
                if name.startswith('results_') or name.startswith('result_reader_'):
                        attribute, n = name.rsplit('_', 1)
                        return getattr(self, attribute)(int(n) - 1)
                return super(DiscoMultiJobTestFixture, self).__getattribute__(name)

        def setUp(self):
                host, port        = self.test_server_address
                self.test_servers = [None] * self.njobs
                self.jobs         = [None] * self.njobs
                for m in xrange(self.njobs):
                        n = m + 1
                        self.test_servers[m] = TestServer.create((host, port + m),
                                                                 getattr(self, 'getdata_%d' % n, self.getdata))
                        self.test_servers[m].start()

                        if not hasattr(self, 'input_%d' % n):
                                setattr(self, 'input_%d' % n, self.input(m))

                        try:
                                self.disco = Disco(self.disco_master_url)
                                jobargs = {'name': '%s_%d' % (self.__class__.__name__, n)}
                                for jobarg in self.jobargs:
                                        jobargname = '%s_%d' % (jobarg, n)
                                        if hasattr(self, jobargname):
                                                jobargs[jobarg] = getattr(self, jobargname)

                                self.jobs[m] = self.disco.new_job(**jobargs)
                                setattr(self, 'job_%d' % n, self.jobs[m])
                        except:
                                for k in xrange(n):
                                        self.test_servers[k].stop()
                                raise

        def tearDown(self):
                for m in xrange(self.njobs):
                        self.test_servers[m].stop()
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
                if issubclass(DiscoTestCase, testCaseClass):
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

