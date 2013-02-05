import string

from disco.job import JobChain
from disco.test import TestCase, TestJob
from disco.util import load_oob
from disco.compat import bytes_to_str, str_to_bytes
from disco.worker.classic.func import default_partition

class OOBJob1(TestJob):
    partitions = 10

    @staticmethod
    def map(e, params):
        k = bytes_to_str(e)
        v = str_to_bytes('value:{0}'.format(k))
        put(k, v)
        yield k, v

    @staticmethod
    def reduce(iter, params):
        for k, v in iter:
            assert v == get(k)
        x = 'reduce:{0}'.format(this_partition())
        put(x, str_to_bytes('value:{0}'.format(x)))
        yield 'all', 'ok'

class OOBJob2(TestJob):
    @staticmethod
    def map(e, params):
        x = bytes_to_str(load_oob(Task.master, params['job'], e))
        assert x == 'value:{0}'.format(e)
        yield 'good', ''

class LargeOOBJob(TestJob):
    @staticmethod
    def map(e, params):
        for i in range(10):
            put('{0}-{1}'.format(e, i), str_to_bytes('val:{0}-{1}'.format(e, i)))
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
        ascii = list(string.ascii_lowercase)
        labels = list(set([default_partition(k, a.partitions, None)
                           for k in ascii]))
        self.assertEquals(sorted(a.oob_list()),
                          sorted(list(string.ascii_lowercase) +
                                 ['reduce:{0}'.format(i) for i in labels]))

    def test_large(self):
        self.job = LargeOOBJob().run(input=['raw://{0}'.format(i)
                                            for i in range(self.num_workers)])
        self.assertResults(self.job, [])
        self.assertEquals(sorted((key, bytes_to_str(self.job.oob_get(key)))
                                 for key in self.job.oob_list()),
                          sorted(('{0}-{1}'.format(i, j), 'val:{0}-{1}'.format(i, j))
                                 for i in range(self.num_workers)
                                 for j in range(10)))
