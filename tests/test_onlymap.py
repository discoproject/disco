from disco.test import DiscoJobTestFixture, DiscoTestCase

class OnlyMapTestCase(DiscoJobTestFixture, DiscoTestCase):
    @property
    def inputs(self):
        return ['%s' % x for x in xrange(100 * self.num_workers)]

    def getdata(self, path):
        return ('%s\n' % path) * 10

    @staticmethod
    def map(e, params):
        return [(e, 1)]

    def runTest(self):
        d = {}
        for k, v in self.results:
            d[k] = d.get(k, 0) + int(v)
        self.assertEquals(len(d), len(self.inputs))
        for input in self.inputs:
            self.assertEquals(d[input], 10)
