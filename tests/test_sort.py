import base64, string

from disco.test import TestCase, TestJob
from disco.util import kvgroup, shuffled
from disco.compat import bytes_to_str, str_to_bytes

alphanum = list(string.ascii_letters) + list(map(str, range(10)))

class SortJob(TestJob):
    scheduler = {'max_cores': 6}
    sort = True

    @staticmethod
    def map(string, params):
        return shuffled((base64.encodestring(str_to_bytes(c)), b'') for c in bytes_to_str(string * 10))

    @staticmethod
    def reduce(iter, params):
        for k, vs in kvgroup(iter):
            yield bytes_to_str(base64.decodestring(k)), len(list(vs))

class SortTestCase(TestCase):
    def serve(self, path):
        return ''.join(alphanum)

    def runTest(self):
        self.job = SortJob().run(input=self.test_server.urls([''] * 100))
        self.assertResults(self.job, sorted((c, 1000) for c in alphanum))
