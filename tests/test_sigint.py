from disco.test import DiscoJobTestFixture, DiscoTestCase

import signal

class InterruptTestCase(DiscoJobTestFixture, DiscoTestCase):
    @property
    def inputs(self):
        return [''] * self.num_workers * 2

    def getdata(self, path):
        return '1 2 3\n'

    @staticmethod
    def map(e, params):
        import time
        time.sleep(10)
        return []

    def runTest(self):
        signal.getsignal(signal.SIGINT)(signal.SIGINT, None)
