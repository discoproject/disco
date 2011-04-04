from datetime import datetime

from disco.test import TestCase, TestJob
from disco.worker.classic.worker import Params

def fun1(a, b):
    return a + b

def fun2(x):
    if x > 10:
        return 1
    return 0

class ParamsJob(TestJob):
    params = Params(x=5, f1=fun1, f2=fun2, now=datetime.now())
    sort = False

    @staticmethod
    def map(e, params):
        yield e, params.f1(int(e), params.x)

    @staticmethod
    def reduce(iter, params):
        for k, v in iter:
            yield k, params.f2(int(v))

class ParamsTestCase(TestCase):
    def serve(self, path):
        return '\n'.join([path] * 10)

    def runTest(self):
        self.job = ParamsJob().run(input=self.test_server.urls(range(10)))
        for k, v in self.results(self.job):
            self.assertEquals(fun2(int(k) + 5), int(v))

