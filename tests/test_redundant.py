from disco.core import result_iterator
from disco.error import DataError
from disco.test import TestCase, TestJob, FailedReply

class RedundantJob(TestJob):
    @staticmethod
    def map(e, params):
        yield int(e), ''

    @staticmethod
    def reduce(iter, params):
        yield sum(k for k, v in iter), ''

class RedundantTestCase(TestCase):
    def serve(self, path):
        if 'fail' in path:
            raise FailedReply()
        return '%s\n' % (int(path) * 10)

    def runTest(self):
        input = ['1', ['2_fail', '2_still_fail', '200'], '3', ['4_fail', '400']]
        self.job = RedundantJob().run(input=self.test_server.urls(input))
        self.assertResults(self.job, [(6040, '')])

class RedundantOutputTestCase(TestCase):
    # This is a tricky test case now that comm.py tries really
    # hard to access the url, which in this case doesn't exist
    # (http://nonode). The test could take almost 10 minutes.
    # We should have a way to lower the number of retries
    # globally.
    """
    def test_unavailable(self):
        from disco.schemes import scheme_raw
        results = list(result_iterator([['http://nonode', 'raw://hello']],
                                       reader=scheme_raw.input_stream))
        self.assertEquals(results, ['hello'])
    """
    def test_corrupt(self):
        def corrupt_reader(fd, size, url, params):
            yield 'hello'
            if 'corrupt' in url:
                raise DataError("Corrupt!", url)
            yield 'there'
        self.assertAllEqual(result_iterator([['raw://corrupt'] * 9 +
                                             ['raw://decent']],
                                            reader=corrupt_reader),
                            ['hello', 'there'])
