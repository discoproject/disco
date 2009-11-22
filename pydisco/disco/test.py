import os
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
from SocketServer import ThreadingMixIn
from httplib import OK, INTERNAL_SERVER_ERROR
from threading import Thread

import disco
from disco.core import Disco, result_iterator
from disco.settings import DiscoSettings
from unittest import TestCase, TestLoader, TextTestRunner

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
                        self.send_header("Content-length", len(data))
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
        map_reader     = staticmethod(disco.func.map_line_reader)
        map_writer     = staticmethod(disco.func.netstr_writer)
        profile        = False
        sort           = False
        mem_sort_limit = 256
        reduce         = None
        reduce_reader  = staticmethod(disco.func.netstr_reader)
        reduce_writer  = staticmethod(disco.func.netstr_writer)
        result_reader  = staticmethod(disco.func.netstr_reader)
        nr_reduces     = 1

        @staticmethod
        def map(*args, **kwargs):
                raise NotImplementedError

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
                        jobargs = {'name':           self.__class__.__name__,
                                   'input':          self.input,
                                   'map':            self.map,
                                   'map_reader':     self.map_reader,
                                   'map_writer':     self.map_writer,
                                   'profile':        self.profile,
                                   'sort':           self.sort,
                                   'mem_sort_limit': self.mem_sort_limit}

                        if self.reduce:
                                jobargs.update({'reduce':        self.reduce,
                                                'reduce_reader': self.reduce_reader,
                                                'reduce_writer': self.reduce_writer,
                                                'nr_reduces':    self.nr_reduces})

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

