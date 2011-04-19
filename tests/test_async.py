from disco.job import JobChain
from disco.test import TestCase, TestJob

class AsyncJob(TestJob):
    partitions = 11
    sort = False

    @staticmethod
    def map(e, params):
        yield e, None

    @staticmethod
    def reduce(iter, params):
        for k, v in iter:
            yield '[%s]' % k, v

class AsyncTestCase(TestCase):
    def sample(self, n):
        from random import sample
        return sample(range(n * 10), n * 2)

    def serve(self, path):
        return '\n'.join([path] * 10)

    def runTest(self):
        N = self.num_workers
        self.job = JobChain((AsyncJob(), self.test_server.urls(self.sample(N)))
                            for x in xrange(5)).wait()
        for job in self.job:
            self.assertEquals(sum(1 for result in self.results(job)), N * 20)
