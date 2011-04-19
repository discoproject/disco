import base64, string

from disco.test import TestCase, TestJob
from disco.util import kvgroup, shuffled

alphanum = list(string.ascii_letters) + map(str, xrange(10))

class SortJob(TestJob):
    sort = True

    @staticmethod
    def map(string, params):
        return shuffled((base64.encodestring(c), '') for c in string * 10)

    @staticmethod
    def reduce(iter, params):
        for k, vs in kvgroup(iter):
            yield base64.decodestring(k), len(list(vs))

class SortTestCase(TestCase):
    def serve(self, path):
        return ''.join(alphanum)

    def runTest(self):
        self.job = SortJob().run(input=self.test_server.urls([''] * 100))
        self.assertResults(self.job, sorted((c, 1000) for c in alphanum))
