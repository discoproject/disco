from disco.test import TestCase, TestJob
from disco.util import kvgroup

class ManyMapJob(TestJob):
    sort = False

    @staticmethod
    def map(line, params):
        for word in line.lower().split():
            yield word, 1

    @staticmethod
    def reduce(iter, params):
        for k, vs in kvgroup(sorted(iter)):
            yield k, sum(int(v) for v in vs)

class ManyMapTestCase(TestCase):
    @property
    def partitions(self):
        return min(self.num_workers * 10, 300)

    def serve(self, path):
        return "Gutta cavat cavat lapidem\n" * 100

    def test_five(self):
        self.job = ManyMapJob().run(input=self.test_server.urls([''] * 5),
                                    partitions=self.partitions)
        self.assertEquals(dict(self.results(self.job)),
                          {'gutta':   int(5e2),
                           'cavat':   int(1e3),
                           'lapidem': int(5e2)})

    def test_50k(self):
        self.job = ManyMapJob().run(input=self.test_server.urls([''] * int(5e4)),
                                    partitions=self.partitions)
        self.assertEquals(dict(self.results(self.job)),
                          {'gutta':   int(5e6),
                           'cavat':   int(1e7),
                           'lapidem': int(5e6)})
