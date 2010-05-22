from disco.test import DiscoMultiJobTestFixture
from disco.test import DiscoJobTestFixture, DiscoTestCase
from disco.error import DiscoError

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
        yield e, 'against_me'

    @property
    def answers(self):
        yield 'organic_vodka', 'against_me'

class MapNonPartitionedOutputTestCase(MapPartitionedOutputTestCase):
    partitions = None

class MapNonPartitionedOutputTestCase2(MapPartitionedOutputTestCase):
    partitions = 0

class ReduceNonPartitionedInputTestCase(DiscoJobTestFixture, DiscoTestCase):
    inputs = ['test']

    def getdata_1(self, path):
        return 'smoothies'

    @staticmethod
    def reduce(iter, out, params):
        for e in iter:
            out.add(e, 'mmm')

    @property
    def answers(self):
        yield 'smoothies', 'mmm'

class ReducePartitionedInputTestCase(DiscoMultiJobTestFixture, DiscoTestCase):
    input_1  = ['raw://sam_adams', 'raw://trader_jose', 'raw://boont_esb']
    njobs    = 2

    @staticmethod
    def map_1(e, params):
        print e
        yield e, None

    @property
    def input_2(self):
        return self.job_1.wait()

    def reduce_2(iter, out, params):
        for k, v in iter:
            print k, v
            out.add(k, v)

    def runTest(self):
        for k, v in self.results_2:
            self.assert_('raw://%s' % k in self.input_1)
            self.assertEquals(v, None)

class MergeReducePartitionedInputTestCase(ReducePartitionedInputTestCase):
    merge_partitions = True
