from disco.test import TestCase, TestJob
import time

class KillJob(TestJob):
    @staticmethod
    def map(e, params):
        time.sleep(30)
        return []

class KillTestCase(TestCase):
    def serve(self, path):
        return '1 2 3\n'

    def runTest(self):
        input = self.test_server.urls([''] * self.num_workers * 2)
        self.job = KillJob().run(input=input)
        self.job.kill()
        time.sleep(1)
        self.assertEquals(self.job.jobinfo()['active'], 'dead')
