import string

from disco.job import JobChain
from disco.test import TestCase, TestJob
from disco.util import load_oob

class OOBJob1(TestJob):
    partitions = 10

    @staticmethod
    def map(e, params):
        v = 'value:%s' % e
        put(e, v)
        yield e, v

    @staticmethod
    def reduce(iter, params):
        for k, v in iter:
            assert v == get(k)
        x = 'reduce:%d' % this_partition()
        put(x, 'value:%s' % x)
        yield 'all', 'ok'

class OOBJob2(TestJob):
    @staticmethod
    def map(e, params):
        x = load_oob(Task.master, params['job'], e)
        assert x == 'value:%s' % e
        yield 'good', ''

class LargeOOBJob(TestJob):
    @staticmethod
    def map(e, params):
        for i in range(10):
            put('%s-%d' % (e, i), 'val:%s-%d' % (e, i))
        return []

class OOBTestCase(TestCase):
    def serve(self, path):
        return path

    def test_chain(self):
        a, b = OOBJob1(), OOBJob2()
        self.job = JobChain({a: [], b: []})
        a.run(input=self.test_server.urls(list(string.ascii_lowercase))).wait()
        b.run(input=['raw://a', 'raw://b', 'raw://c'],
              params={'job': a.name})
        self.assertResults(b, [('good', '')] * 3)
        self.assertEquals(sorted(a.oob_list()),
                          sorted(list(string.ascii_lowercase) +
                                 ['reduce:%s' % i for i in xrange(a.partitions)]))

    def test_large(self):
        self.job = LargeOOBJob().run(input=['raw://%d' % i
                                            for i in range(self.num_workers)])
        self.assertResults(self.job, [])
        self.assertEquals(sorted((key, self.job.oob_get(key))
                                 for key in self.job.oob_list()),
                          sorted(('%d-%d' % (i, j), 'val:%d-%d' % (i, j))
                                 for i in range(self.num_workers)
                                 for j in range(10)))
