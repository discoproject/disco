import time

from disco.core import JobError
from disco.test import TestCase, TestJob
from disco.util import urlsplit
from disco.worker.classic.func import string_input_stream

class ForceLocalJob(TestJob):
    scheduler = {'force_local': True}

    def map_input_stream(stream, size, url, params):
        scheme, netloc, path = urlsplit(url)
        assert netloc.host == Task.host
        return string_input_stream(str(netloc), size, url, params)
    map_input_stream = [map_input_stream]

    @staticmethod
    def map(e, params):
        time.sleep(0.2)
        yield e, ''

class ForceRemoteJob(ForceLocalJob):
    scheduler = {'force_remote': True}

    def map_input_stream(stream, size, url, params):
        from disco.func import string_input_stream
        from disco.util import urlsplit
        scheme, netloc, path = urlsplit(url)
        assert netloc.host != Task.host
        return string_input_stream(str(netloc), size, url, params)
    map_input_stream = [map_input_stream]

class ForceSchedulerTestCase(TestCase):
    fake_input = ['foobar://nonodenamedthishopefully_ifnotthistestwillfail']

    @property
    def real_input(self):
        return ['http://%s' % node
                for node, max_workers in self.nodes.iteritems()
                for x in xrange(max_workers * 2)]

    def assertResults(self, job, input):
        self.assertAllEqual(sorted(self.results(job)),
                            sorted((str(urlsplit(i)[1]), '') for i in input))

    def test_force_local(self):
        self.job = ForceLocalJob().run(input=self.real_input)
        self.assertResults(self.job, self.real_input)

    def test_force_local_no_node(self):
        self.job = ForceLocalJob().run(input=self.fake_input)
        self.assertRaises(JobError, self.job.wait)

    def test_force_remote_no_node(self):
        self.job = ForceRemoteJob().run(input=self.fake_input)
        self.assertResults(self.job, self.fake_input)

    def test_force_remote(self):
        if len(self.nodes) < 2:
            self.skipTest("Cannot test force remote with < 2 nodes")
        else:
            self.job = ForceRemoteJob().run(input=self.real_input)
            self.assertResults(self.job, self.real_input)
