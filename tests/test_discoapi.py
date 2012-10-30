from disco.test import TestCase, TestJob

special = '--special_test_string--'

def isspecial(offset__time_node_message):
    return special in offset__time_node_message[1][2]

class DiscoAPIJob(TestJob):
    @staticmethod
    def map(e, params):
        for i in range(3):
            print(special)
        yield e.split('_')

class DiscoAPITestCase(TestCase):
    def runTest(self):
        self.job = DiscoAPIJob().run(input=['raw://disco_api'])
        self.assertResults(self.job, [('disco', 'api')])
        self.assertEquals(len(list(filter(isspecial, self.job.events()))), 3)
