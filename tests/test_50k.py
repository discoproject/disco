from disco.test import DiscoJobTestFixture, DiscoTestCase

class FiveTestCase(DiscoJobTestFixture, DiscoTestCase):
    inputs = [''] * int(5)
    sort   = False

    def getdata(self, path):
        return "Gutta cavat cavat capidem\n" * 100

    @property
    def partitions(self):
        return min(self.num_workers * 10, 300)

    @staticmethod
    def map(e, params):
        return [(w, 1) for w in re.sub(r'\W', ' ', e).lower().split()]

    @staticmethod
    def reduce(iter, out, params):
        from itertools import groupby
        for k, kvs in groupby(sorted(iter), lambda kv: kv[0]):
            out.add(k, sum(int(v) for k, v in kvs))

    def runTest(self):
        self.assertEquals(dict(self.results),
                  {'gutta':   int(5e2),
                   'cavat':   int(1e3),
                   'capidem': int(5e2)})

class FiftyThousandTestCase(FiveTestCase):
    inputs = [''] * int(5e4)

    def runTest(self):
        self.assertEquals(dict(self.results),
                  {'gutta':   int(5e6),
                   'cavat':   int(1e7),
                   'capidem': int(5e6)})
