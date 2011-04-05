import signal, time

from disco.test import TestCase, TestJob

class InterruptJob(TestJob):
    @staticmethod
    def map(e, params):
        time.sleep(10)
        return []

class InterruptTestCase(TestCase):
    def serve(self, path):
        return '1 2 3\n'

    def runTest(self):
        input = [''] * self.num_workers * 2
        self.job = InterruptJob().run(input=self.test_server.urls(input))
        signal.getsignal(signal.SIGINT)(signal.SIGINT, None)
