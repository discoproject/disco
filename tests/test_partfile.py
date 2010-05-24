from disco.test import DiscoJobTestFixture, DiscoTestCase

from collections import defaultdict

class PartitionFileTestCase(DiscoJobTestFixture, DiscoTestCase):
    inputs   = ['%s:%s' % i for x in xrange(10)
            for i in zip([x] * 10, range(x, x + 10))]
    def getdata(self, path):
        return '%s\n' % path

    @staticmethod
    def map(e, params):
        return [e.split(':')]

    @property
    def answers(self):
        for x in xrange(10):
            yield '%s' % x, sum(xrange(x, x + 10))

    def runTest(self):
        results = defaultdict(int)
        for k, v in self.results:
            results[k] += int(v)
        self.assertEquals(results, dict(self.answers))

class MultiPartitionFileTestCase(PartitionFileTestCase):
    @property
    def partitions(self):
        return len(self.inputs)
