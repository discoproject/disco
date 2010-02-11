from disco.test import DiscoJobTestFixture, DiscoTestCase
from disco.core import Params

from datetime import datetime

def fun1(a, b):
    return a + b

def fun2(x):
    if x > 10:
        return 1
    return 0

class ParamsTestCase(DiscoJobTestFixture, DiscoTestCase):
    inputs = range(10)
    params = Params(x=5, f1=fun1, f2=fun2, now=datetime.now())
    sort   = False

    def getdata(self, path):
        return '\n'.join([path] * 10)

    @staticmethod
    def map(e, params):
        return [(e, params.f1(int(e), params.x))]

    @staticmethod
    def reduce(iter, out, params):
        for k, v in iter:
            out.add(k, params.f2(int(v)))

    def runTest(self):
        for k, v in self.results:
            self.assertEquals(fun2(int(k) + 5), int(v))

