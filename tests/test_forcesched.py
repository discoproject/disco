from disco.test import DiscoJobTestFixture, DiscoTestCase
from disco.core import JobError
from disco.util import urlsplit

class ForceLocalTestCase(DiscoJobTestFixture, DiscoTestCase):
    scheduler = {'force_local': True}

    @property
    def input(self):
        return ['http://%s' % node
            for node, max_workers in self.nodes.iteritems()
            for x in xrange(max_workers * 2)]

    def map_input_stream(stream, size, url, params):
        from disco.func import string_input_stream
        from disco.util import urlsplit
        scheme, netloc, path = urlsplit(url)
        assert netloc == Task.netloc
        return string_input_stream(str(netloc), size, url, params)
    map_input_stream = [map_input_stream]

    @staticmethod
    def map(e, params):
        time.sleep(0.2)
        return [(e, '')]

    @property
    def answers(self):
        for input in self.input:
            scheme, netloc, path = urlsplit(input)
            yield str(netloc), ''

    def runTest(self):
        for result, answer in zip(sorted(self.answers), sorted(self.results)):
            self.assertEquals(result, answer)

class ForceLocalNoNodeTestCase(ForceLocalTestCase):
    input = ['foobar://nonodenamedthishopefully_ifnotthistestwillfail']

    def runTest(self):
        self.assertRaises(JobError, self.job.wait)

class ForceRemoteNoNodeTestCase(ForceLocalTestCase):
    input     = ['foobar://nonodenamedthishopefully_ifnotthistestwillfail']
    scheduler = {'force_remote': True}

    def map_input_stream(stream, size, url, params):
        from disco.func import string_input_stream
        from disco.util import urlsplit
        scheme, netloc, path = urlsplit(url)
        assert netloc != Task.netloc
        return string_input_stream(str(netloc), size, url, params)
    map_input_stream = [map_input_stream]

class ForceRemoteTestCase(ForceRemoteNoNodeTestCase):
    def runTest(self):
        if len(self.nodes) > 1:
            return super(ForceRemoteTestCase, self).runTest()
        self.skipTest("Cannot test force remote with < 2 nodes")
