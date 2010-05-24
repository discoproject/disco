from disco.test import DiscoMultiJobTestFixture, DiscoTestCase
from disco.error import JobError

class MapResultsTestCase(DiscoMultiJobTestFixture, DiscoTestCase):
    njobs = 2
    inputs_1 = ['huey', 'dewey', 'louie']
    partitions_1 = 3

    def getdata(self, path):
        return path + "\n"

    @property
    def input_2(self):
        try:
            self.job_1.wait()
        except JobError:
            return self.job_1.mapresults()

    @staticmethod
    def map_1(e, params):
        if type(e) == tuple:
            e = e[0]
        yield (e + "!", '')

    @staticmethod
    def reduce_1(iter, out, params):
        raise Exception("This is supposed to fail")

    @staticmethod
    def reduce_2(iter, out, params):
        for k, v in iter:
            out.add(k + "?", v)

    @property
    def answers(self):
        return [('dewey!?', ''), ('huey!?', ''), ('louie!?', '')]

    def runTest(self):
        self.assertEquals(self.answers, sorted(list(self.results_2)))







