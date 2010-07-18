from disco.test import DiscoJobTestFixture, DiscoTestCase, FailedReply
from disco.core import result_iterator

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

class RedundantOutputTestCase(DiscoTestCase):
    def test_unavailable(self):
        from disco.schemes import scheme_raw
        results = list(result_iterator([['http://nonode', 'raw://hello']],
                                       reader=scheme_raw.input_stream))
        self.assertEquals(results, ['hello'])

    def test_corrupt(self):
        def corrupt_reader(fd, size, url, params):
            if 'corrupt' in url:
                yield 'hello'
                raise Exception("Corrupt!")
            elif 'decent' in url:
                yield '-skip-'
                yield 'there'
        results = list(result_iterator([['raw://corrupt', 'raw://decent']],
                                       reader=corrupt_reader))
        self.assertEquals(results, ['hello', 'there'])
