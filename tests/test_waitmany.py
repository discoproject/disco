from disco.error import JobError
from disco.job import JobChain
from disco.test import TestCase, TestJob

class WaitJob1(TestJob):
    @staticmethod
    def map(e, params):
        time.sleep(.1)
        return []

class WaitJob2(TestJob):
    @staticmethod
    def map(e, params):
        raise ValueError("This job is supposed to fail.")

class WaitManyTestCase(TestCase):
    def serve(self, path):
        return 'foo'

    def runTest(self):
        input = self.test_server.urls([''] * 5)
        a, b, c = WaitJob1(), WaitJob1(), WaitJob2()
        self.job = JobChain({a: input,
                             b: input,
                             c: input})
        self.assertRaises(JobError, self.job.wait)
        valid = JobChain({a: input, b:input})
        self.assertEquals(valid.wait(), valid)

