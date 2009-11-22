from disco.test import DiscoJobTestFixture, DiscoTestCase

class KillTestCase(DiscoJobTestFixture, DiscoTestCase):
        @property
        def inputs(self):
                num = sum(x['max_workers'] for x in self.disco.nodeinfo()['available'])
                return [''] * num * 2

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
