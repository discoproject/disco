from disco.test import DiscoJobTestFixture, DiscoTestCase, FailedReply

class RedundantTestCase(DiscoJobTestFixture, DiscoTestCase):
    inputs = ['1', ['2_fail', '2_still_fail', '200'], '3', ['4_fail', '400']]

    def getdata(self, path):
        if 'fail' in path:
            raise FailedReply()
        return '%s\n' % (int(path) * 10)

    @staticmethod
    def map(e, params):
        return [(e, '')]

    @staticmethod
    def reduce(iter, out, params):
        out.add(sum(int(k) for k, v in iter), '')

    @property
    def answers(self):
        yield 6040, ''
