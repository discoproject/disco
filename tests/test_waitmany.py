from disco.test import DiscoMultiJobTestFixture, DiscoTestCase
from disco.core import JobError

class WaitManyTestCase(DiscoMultiJobTestFixture, DiscoTestCase):
    njobs  = 3
    inputs_1 = inputs_2 = inputs_3 = [''] * 5

    def getdata(self, path):
        return 'foo'

    @staticmethod
    def map_1(e, params):
        time.sleep(.1)
        return []

    @staticmethod
    def map_2(e, params):
        time.sleep(.2)
        return []

    @staticmethod
    def map_3(e, params):
        err('this job is supposed to fail')

    def runTest(self):
        res, jobs = [], self.jobs
        while jobs:
            ready, jobs = self.disco.results(jobs)
            res += ready
        self.assertRaises(JobError, self.job_3.wait)
        self.assertEquals(len(res), 3)
