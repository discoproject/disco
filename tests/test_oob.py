from disco.test import DiscoMultiJobTestFixture, DiscoTestCase
from disco.core import Params

import string

class OOBTestCase(DiscoMultiJobTestFixture, DiscoTestCase):
    njobs    = 2
    inputs_1     = list(string.ascii_lowercase)
    inputs_2     = ['raw://a', 'raw://b', 'raw://c']
    partitions_1 = 10

    @property
    def params_2(self):
        return Params(job=self.job_1.name)

    def getdata_1(self, path):
        return path

    @staticmethod
    def map_1(e, params):
        v = 'value:%s' % e
        put(e, v)
        return [(e, v)]

    @staticmethod
    def reduce_1(iter, out, params):
        for k, v in iter:
            assert v == get(k)
        x = 'reduce:%d' % this_partition()
        put(x, 'value:%s' % x)
        out.add('all', 'ok')

    @property
    def input_2(self):
        self.job_1.wait()
        return self.inputs_2

    @staticmethod
    def map_2(e, params):
        x = get(e, params.job)
        assert x == 'value:%s' % e
        return [('good', '')]

    def runTest(self):
        super(OOBTestCase, self).runTest()
        self.assertEquals(sorted(self.job_1.oob_list()), sorted(self.oob_data))

    @property
    def answers_1(self):
        return [('all', 'ok')] * self.partitions_1

    @property
    def answers_2(self):
        return [('good', '')] * len(self.inputs_2)

    @property
    def oob_data(self):
        for i in self.inputs_1:
            yield '%s' % i
        for i in xrange(self.partitions_1):
            yield 'reduce:%s' % i
