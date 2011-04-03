from disco.job import JobChain
from disco.test import TestCase, TestJob

from disco.worker.classic import func

class ChainJobA(TestJob):
    partitions = 4
    params = {'suffix': '0'}
    sort = False

    @staticmethod
    def map(e, params):
        yield e.strip() + params['suffix'], 0

    @staticmethod
    def reduce(iter, params):
        for k, v in iter:
            yield k + '-', v

class ChainJobB(TestJob):
    map_reader = staticmethod(func.chain_reader)
    partitions = 4
    params = {'suffix': '1'}
    sort = False

    @staticmethod
    def map((k, v), params):
        yield k + params['suffix'], v + 1

    reduce = staticmethod(ChainJobA.reduce)

class ChainTestCase(TestCase):
    animals = ['horse', 'sheep', 'whale', 'tiger']

    def serve(self, path):
        return '\n'.join(self.animals)

    def runTest(self):
        a, b = ChainJobA(), ChainJobB()
        self.job = JobChain({a: self.test_server.urls([''] * 100),
                             b: a}).wait()
        for key, value in self.results(b):
            self.assert_(key[:5] in self.animals)
            self.assertEquals(key[5:], '0-1-')
            self.assertEquals(value, 1)

class DavinChainJobA(TestJob):
    @staticmethod
    def map(e, params):
        yield e, ''

class DavinChainJobC(TestJob):
    reduce = staticmethod(func.nop_reduce)

class DavinChainTestCase(TestCase):
    def runTest(self):
        a, b, c = DavinChainJobA(), DavinChainJobA(), DavinChainJobC()
        self.job = JobChain({a: ['raw://0', 'raw://1', 'raw://2'],
                             b: ['raw://3', 'raw://4', 'raw://5'],
                             c: [a, b]}).wait()
        self.assertAllEqual(sorted(self.results(c)),
                            ((str(x), '') for x in xrange(6)))
