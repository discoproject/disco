from disco.test import TestCase, TestJob

class KillJob(TestJob):
    @staticmethod
    def map(e, params):
        import time
        time.sleep(30)
        return []

class KillTestCase(TestCase):
    def serve(self, path):
        return '1 2 3\n'

    def runTest(self):
        input = self.test_server.urls([''] * self.num_workers * 2)
        self.job = KillJob().run(input=input)
        self.job.kill()
        self.assertEquals(self.job.jobinfo()['active'], 'dead')
