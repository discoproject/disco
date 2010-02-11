from disco.test import DiscoJobTestFixture, DiscoTestCase
from disco.core import func

from math import pi

class ObjectRWTestCase(DiscoJobTestFixture, DiscoTestCase):
    inputs       = ['01/11/1965', '14/03/1983', '12/12/2002']
    map_writer       = func.object_writer
    reduce_reader    = func.object_reader
    reduce_writer    = func.object_writer
    result_reader    = staticmethod(func.object_reader)
    sort         = False

    def getdata(self, path):
        return ('%s\n' % path) * 10

    @staticmethod
    def map(e, params):
        return [({'PI': math.pi}, time.strptime(e, '%d/%m/%Y'))]

    @staticmethod
    def reduce(iter, out, params):
        for k, v in iter:
            out.add({'PI2': k['PI']}, datetime.datetime(*v[0:6]))

    def runTest(self):
        for n, (k, v) in enumerate(self.results):
            self.assertEquals(k['PI2'], pi)
            self.assert_(v.strftime('%d/%m/%Y') in self.inputs)
        self.assertEquals(n, len(self.inputs) * 10 - 1)
