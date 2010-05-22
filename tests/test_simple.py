from disco.test import DiscoJobTestFixture, DiscoTestCase

class SimpleTestCase(DiscoJobTestFixture, DiscoTestCase):
    inputs = [3, 5, 7, 11, 13, 17, 19, 23, 29, 31]

    def getdata(self, path):
        return '\n'.join([path] * 10)

    @staticmethod
    def map(e, params):
        return [('=' + e, e)]

    @staticmethod
    def reduce(iter, out, params):
        s = 1
        for k, v in iter:
            assert k == '=' + v, "Corrupted key!"
            s *= int(v)
        out.add('result', s)

    @property
    def answers(self):
        yield ('result',
               102838057849351261119838300575805205791938675762040158350002406688858214958513887550465113168573010369619140625)
