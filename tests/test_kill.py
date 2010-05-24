from disco.test import DiscoJobTestFixture, DiscoTestCase

class KillTestCase(DiscoJobTestFixture, DiscoTestCase):
    @property
    def inputs(self):
        return [''] * self.num_workers * 2

    def getdata(self, path):
        return '1 2 3\n'

    @staticmethod
    def map(e, params):
        import time
        time.sleep(30)
        return []

    def runTest(self):
        self.job.kill()
        self.assertEquals(self.job.jobinfo()['active'], 'dead')
