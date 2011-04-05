from disco.test import TestCase, TestJob
from disco.error import JobError
from disco.events import Status

class PrintJob(TestJob):
    @staticmethod
    def map(e, params):
        print e
        return []

class StatusJob(TestJob):
    @staticmethod
    def map(e, params):
        Status("Internal msg").send()
        return []

class RateLimitTestCase(TestCase):
    inputs = [1]

    def serve(self, path):
        return 'badger\n' * 1000

    def test_print(self):
        self.job = PrintJob().run(input=self.test_server.urls([1]))
        self.assertRaises(JobError, self.job.wait)
        self.assertEquals(self.job.jobinfo()['active'], 'dead')

    def test_status(self):
        self.job = StatusJob().run(input=self.test_server.urls([1]))
        self.job.wait()
        self.assertEquals(self.job.jobinfo()['active'], 'ready')
