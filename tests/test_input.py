from disco.test import DiscoMultiJobTestFixture
from disco.test import DiscoJobTestFixture, DiscoTestCase
from disco.error import JobError
from disco import func

class EmptyInputTestCase(DiscoJobTestFixture, DiscoTestCase):
    input = []

    @staticmethod
    def map(e, params):
        return []

    @property
    def answers(self):
        return []

class MapPartitionedOutputTestCase(DiscoJobTestFixture, DiscoTestCase):
    input = ['raw://organic_vodka']
    partitions = 2

    @staticmethod
    def map(e, params):
        assert Task.jobdict['partitions'] == 2
        yield e, 'against_me'

    @property
    def answers(self):
        yield 'organic_vodka', 'against_me'

class MapNonPartitionedOutputTestCase(MapPartitionedOutputTestCase):
    partitions = None

    @staticmethod
    def map(e, params):
        assert Task.jobdict['partitions'] == 0
        yield e, 'against_me'

class MapNonPartitionedOutputTestCase2(MapPartitionedOutputTestCase):
    partitions = 0

    @staticmethod
    def map(e, params):
        assert Task.jobdict['partitions'] == 0
        yield e, 'against_me'

class ReduceNonPartitionedInputTestCase(DiscoJobTestFixture, DiscoTestCase):
    inputs        = ['test']
    reduce_reader = func.map_line_reader

    def getdata(self, path):
        return 'smoothies'

    @staticmethod
    def reduce(iter, out, params):
        for e in iter:
            out.add(e, 'mmm')

    @property
    def answers(self):
        yield 'smoothies', 'mmm'

class MapReducePartitionedTestCase(ReduceNonPartitionedInputTestCase):
    partitions = 8
    reduce_reader = func.chain_reader

    @staticmethod
    def map(e, params):
        yield 'smoothies', 'mmm'

    @staticmethod
    def reduce(iter, out, params):
        for e in iter:
            out.add(*e)

class ReducePartitionedInputTestCase(DiscoMultiJobTestFixture, DiscoTestCase):
    beers        = ['sam_adams', 'trader_jose', 'boont_esb']
    input_1      = ['raw://%s' % beer for beer in beers]
    input_2      = input_1
    njobs        = 3
    partitions_1 = 3
    partitions_2 = 3

    @staticmethod
    def map_1(e, params):
        yield e, None

    @staticmethod
    def map_2(e, params):
        yield e, None

    @property
    def input_3(self):
        return self.job_1.wait() + self.job_2.wait()

    def reduce_3(iter, out, params):
        for k, v in iter:
            out.add(k, v)

    def runTest(self):
        answers = sorted(self.beers * 2)
        results = sorted(k for k, v in self.results_3 if v is None)
        for answer, result in zip(answers, results):
            self.assertEquals(answer, result)
        self.assertEquals(len(answers), len(results))

class MismatchedPartitionedInputTestCase(ReducePartitionedInputTestCase):
    partitions_1 = 2

    def runTest(self):
        self.assertRaises(JobError, self.job_3.wait)

class MergeReducePartitionedInputTestCase(ReducePartitionedInputTestCase):
    merge_partitions_3 = True

class MergeReducePartitionedInputTestCase2(ReducePartitionedInputTestCase):
    merge_partitions_3 = True
    partitions_1 = 2
