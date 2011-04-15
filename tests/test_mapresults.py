from disco.core import result_iterator
from disco.test import TestCase, TestJob

class MapResultsJob(TestJob):
    partitions = 3

    @staticmethod
    def map(e, params):
        yield e + '!', ''

    @staticmethod
    def reduce(iter, params):
        for k, v in iter:
            yield k + '?', v


class MapResultsTestCase(TestCase):
    def runTest(self):
        ducks = ['huey', 'dewey', 'louie']
        self.job = MapResultsJob().run(input=['raw://%s' % d for d in ducks])
        self.assertAllEqual(sorted(result_iterator(self.job.wait())),
                            sorted(('%s!?' % d, '') for d in ducks))
        self.assertAllEqual(sorted(result_iterator(self.job.mapresults())),
                            sorted(('%s!' % d, '') for d in ducks))






