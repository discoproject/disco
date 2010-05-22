from disco.test import DiscoJobTestFixture, DiscoTestCase

from cStringIO import StringIO

class ProfileTestCase(DiscoJobTestFixture, DiscoTestCase):
    inputs     = [''] * 1
    partitions = 30
    profile    = True
    sort       = False

    def getdata(self, path):
        return "Gutta cavat cavat capidem\n" * 10

    @staticmethod
    def map(e, params):
        return [(w, 1) for w in re.sub("\W", " ", e).lower().split()]

    @staticmethod
    def reduce(iter, out, params):
        from itertools import groupby
        for k, kvs in groupby(sorted(iter), lambda kv: kv[0]):
            out.add(k, sum(int(v) for k, v in kvs))

    def runTest(self):
        self.assertEquals(dict(self.results), self.answers)
        self.job.profile_stats().print_stats()

    @property
    def answers(self):
        return {'gutta': 10, 'cavat': 20, 'capidem': 10}
