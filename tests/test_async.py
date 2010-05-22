from disco.test import DiscoMultiJobTestFixture, DiscoTestCase

from random import sample

class AsyncTestCase(DiscoMultiJobTestFixture, DiscoTestCase):
    njobs      = 5
    partitions = 11
    sort       = False

    def input(self, m):
        num_workers = self.num_workers
        inputs = sample(range(num_workers * 10), num_workers * 2)
        return self.test_servers[m].urls(inputs)

    def getdata(self, path):
        return '\n'.join([path] * 10)

    @staticmethod
    def map(e, params):
        return [(e, None)]

    map_1 = map_2 = map_3 = map_4 = map_5 = map

    @staticmethod
    def reduce(iter, out, params):
        for k, v in iter:
            out.add('[%s]' % k, v)

    reduce_1 = reduce_2 = reduce_3 = reduce_4 = reduce_5 = reduce

    def runTest(self):
        for m in xrange(self.njobs):
            self.assertEquals(sum(1 for result in self.results(m)),
                      self.num_workers * 20)
