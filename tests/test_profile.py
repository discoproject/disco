import cStringIO
from disco.test import TestCase, TestJob
from disco.util import kvgroup

class ProfileJob(TestJob):
    partitions = 30
    profile    = True
    sort       = False

    @staticmethod
    def map(e, params):
        return [(w, 1) for w in re.sub('\W', ' ', e).lower().split()]

    @staticmethod
    def reduce(iter, params):
        for k, vs in kvgroup(sorted(iter)):
            yield k, sum(int(v) for v in vs)

class ProfileTestCase(TestCase):
    def serve(self, path):
        return "Gutta cavat cavat lapidem\n" * 10

    def reduce_calls(self):
        out = cStringIO.StringIO()
        self.job.profile_stats(stream=out).print_stats('worker', 'reduce')
        for line in out.getvalue().splitlines():
            if 'worker.py' in line:
                return int(line.strip().split()[0])

    def runTest(self):
        self.job = ProfileJob().run(input=self.test_server.urls(['']))
        self.assertEquals(dict(self.results(self.job)),
                          {'gutta': 10, 'cavat': 20, 'lapidem': 10})
        self.assertEquals(self.reduce_calls(), ProfileJob.partitions)
