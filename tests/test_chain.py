from disco.test import DiscoMultiJobTestFixture, DiscoTestCase
from disco.core import Params
from disco.func import chain_reader

import string

class ChainTestCase(DiscoMultiJobTestFixture, DiscoTestCase):
    animals      = ['horse', 'sheep', 'whale', 'tiger']

    njobs    = 2
    inputs_1     = [''] * 100
    map_reader_2 = chain_reader
    nr_reduces_1 = 4
    nr_reduces_2 = 4
    params_1     = {'suffix': '0'}
    params_2     = {'suffix': '1'}
    sort_1       = False
    sort_2       = False

    def getdata_1(self, path):
        return '\n'.join(self.animals)

    @staticmethod
    def map_1(e, params):
        return [(e + params['suffix'], 0)]

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
            self.assertEquals(value, '1')



