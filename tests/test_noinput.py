from disco.test import DiscoJobTestFixture, DiscoTestCase

class NoInputTestCase(DiscoJobTestFixture, DiscoTestCase):
    inputs = []

    def getdata(self, path):
        return '\n'.join([path] * 10)

    @staticmethod
    def map(e, params):
        yield '=' + e, e

    @staticmethod
    def reduce(iter, out, params):
        s = 1
        for k, v in iter:
            assert k == '=' + v, "Corrupted key!"
            s *= int(v)
        out.add('result', s)

    @property
    def answers(self):
        yield None
