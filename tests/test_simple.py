from disco.test import DiscoJobTestFixture, DiscoTestCase

class SimpleTestCase(DiscoJobTestFixture, DiscoTestCase):
    inputs = [3, 5, 7, 11, 13, 17, 19, 23, 29, 31]

    def getdata(self, path):
        return '\n'.join([path] * 10)

    @staticmethod
    def map(e, params):
        yield int(e), e.strip()

    @staticmethod
    def reduce(iter, out, params):
        for k, v in sorted(iter):
            out.add(k, v)

    @property
    def answers(self):
        return ((i, str(i)) for i in self.inputs for x in xrange(10))

class SimplerTestCase(SimpleTestCase):
    @staticmethod
    def reduce(iter, params):
        return sorted(iter)
