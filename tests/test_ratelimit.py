from disco.test import TestCase, TestJob
from disco.error import JobError

class PrintJob(TestJob):
    @staticmethod
    def map(e, params):
        import sys, disco.json
        msg = disco.json.dumps(e)
        sys.stderr.write('MSG %d %s\n' % (len(msg), msg))
        return []

class RateLimitTestCase(TestCase):
    inputs = [1]

    def serve(self, path):
        return 'badger\n' * 1000

    def test_print(self):
        self.job = PrintJob().run(input=self.test_server.urls([1]))
        self.assertRaises(JobError, self.job.wait)
        self.assertEquals(self.job.jobinfo()['active'], 'dead')
