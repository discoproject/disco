from disco.test import TestCase, TestJob
from disco.util import kvgroup

class OnlyMapJob(TestJob):
    @staticmethod
    def map(e, params):
        yield int(e), 1

class OnlyReduceJob(TestJob):
    reduce_reader = None

    @staticmethod
    def reduce(iter, params):
        yield '', sum(int(item) for item in iter)

class FlowTestCase(TestCase):
    def serve(self, path):
        return '\n'.join([path] * 10)

    def test_map(self):
        input = xrange(10 * self.num_workers)
        self.job = OnlyMapJob().run(input=self.test_server.urls(input))
        results = kvgroup(sorted(self.results(self.job)))
        self.assertAllEqual(((k, sum(vs)) for k, vs in results),
                            ((i, 10) for i in input))

    def test_reduce(self):
        input = xrange(10 * self.num_workers)
        self.job = OnlyReduceJob().run(input=self.test_server.urls(input))
        self.assertResults(self.job, [('', sum(input) * 10)])
