from disco.test import DiscoMultiJobTestFixture, DiscoTestCase
from disco.core import Params
from disco.func import chain_reader, nop_reduce

import string

class ChainTestCase(DiscoMultiJobTestFixture, DiscoTestCase):
    animals      = ['horse', 'sheep', 'whale', 'tiger']

    njobs        = 2
    inputs_1     = [''] * 100
    map_reader_2 = chain_reader
    partitions_1 = 4
    partitions_2 = 4
    params_1     = {'suffix': '0'}
    params_2     = {'suffix': '1'}
    sort_1       = False
    sort_2       = False

    def getdata_1(self, path):
        return '\n'.join(self.animals)

    @staticmethod
    def map_1(e, params):
        yield e.strip() + params['suffix'], 0

    @staticmethod
    def reduce_1(iter, out, params):
        for k, v in iter:
            out.add(k + "-", v)

    @staticmethod
    def map_2(e, params):
        return [(e[0] + params['suffix'], int(e[1]) + 1)]

    reduce_2 = reduce_1

    @property
    def input_2(self):
        return self.job_1.wait()

    def runTest(self):
        for key, value in self.results_2:
            self.assert_(key[:5] in self.animals)
            self.assertEquals(key[5:], '0-1-')
            self.assertEquals(value, 1)


class DavinChainTestCase(DiscoMultiJobTestFixture, DiscoTestCase):
    njobs        = 3
    input_1      = ['raw://0', 'raw://1', 'raw://2']
    input_2      = ['raw://3', 'raw://4', 'raw://5']
    map_reader_3 = chain_reader
    reduce_3     = nop_reduce
    # assert this fails sometime:
    # partitions_3 = 2

    def map_1(e, params):
        yield e, ''

    map_2 = map_1

    @property
    def input_3(self):
        return self.job_1.wait() + self.job_2.wait()

    def runTest(self):
        for n, (key, value) in zip(xrange(6), sorted(self.results_3)):
            self.assertEquals(n, int(key))
