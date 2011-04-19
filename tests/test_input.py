from disco.job import JobChain
from disco.error import JobError
from disco.test import TestCase, TestJob
from disco.worker.classic import func

class MapJob(TestJob):
    partitions = 3

    @staticmethod
    def map(e, params):
        yield e, 'against_me'

class ReduceJob(TestJob):
    @staticmethod
    def reduce(iter, params):
        for item in iter:
            yield item, 'mmm'

class MergeReduceJob(ReduceJob):
    merge_partitions = True

class MapReduceJob(MapJob, ReduceJob):
    pass

class InputTestCase(TestCase):
    def serve(self, path):
        return 'smoothies'

    def test_empty_map(self):
        self.job = MapJob().run(input=[])
        self.assertResults(self.job, [])

    def test_empty_reduce(self):
        self.job = ReduceJob().run(input=[])
        self.assertResults(self.job, [])

    def test_empty_mapreduce(self):
        self.job = MapReduceJob().run(input=[])
        self.assertResults(self.job, [])

    def test_partitioned_map(self):
        self.job = MapJob().run(input=['raw://organic_vodka'], partitions=2)
        self.assertResults(self.job, [('organic_vodka', 'against_me')])

    def test_nonpartitioned_map(self):
        self.job = MapJob().run(input=['raw://organic_vodka'], partitions=None)
        self.assertResults(self.job, [('organic_vodka', 'against_me')])

    def test_nonpartitioned_reduce(self):
        self.job = ReduceJob().run(input=self.test_server.urls(['test']),
                                   partitions=None,
                                   reduce_reader=None)
        self.assertResults(self.job, [('smoothies', 'mmm')])

    def test_partitioned_mapreduce(self):
        self.job = MapReduceJob().run(input=self.test_server.urls(['test']),
                                      partitions=8,
                                      reduce_reader=func.chain_reader)
        self.assertResults(self.job, [(('smoothies', 'against_me'), 'mmm')])

    def test_partitioned_reduce(self):
        beers = ['sam_adams', 'trader_jose', 'boont_esb']
        input = ['raw://%s' % beer for beer in beers]
        a, b, c, d = MapJob(), MapJob(), ReduceJob(), MergeReduceJob()
        self.job = JobChain({a: input,
                             b: input,
                             c: [a, b],
                             d: [a, b]}).wait()
        self.assertAllEqual(sorted(self.results(c)), sorted(self.results(d)))
