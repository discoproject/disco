from disco.test import DiscoJobTestFixture, DiscoTestCase
from disco.core import Params

class InitTestCase(DiscoJobTestFixture, DiscoTestCase):
    inputs = range(10)
    params = Params(x=10)
    sort   = False

    def getdata(self, path):
        return 'skipthis\n' + ('%s\n' % path) * 10

    @staticmethod
    def map_init(input_iter, params):
        input_iter.next()
        params.x += 100

    @staticmethod
    def map(e, params):
        return [(e, int(e) + params.x)]

    @staticmethod
    def reduce_init(input_iter, params):
        params.y = 1000

    @staticmethod
    def reduce(iter, out, params):
        for k, v in iter:
            out.add(k, int(v) + params.y)

    def runTest(self):
        results = list(self.results)
        for k, v in results:
            self.assertEquals(int(k) + 1110, int(v))
        self.assertEquals(len(results), 100)
