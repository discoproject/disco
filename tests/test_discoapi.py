from disco.test import TestCase, TestJob

special = '--special_test_string--'

def isspecial((offset, (time, node, message))):
    return special in message

class DiscoAPIJob(TestJob):
    @staticmethod
    def map(e, params):
        for i in range(3):
            print special
        yield e.split('_')

class DiscoAPITestCase(TestCase):
    def runTest(self):
        self.job = DiscoAPIJob().run(input=['raw://disco_api'])
        self.assertResults(self.job, [('disco', 'api')])
        self.assertEquals(len(filter(isspecial, self.job.events())), 3)
