from disco.test import DiscoJobTestFixture, DiscoTestCase
from disco.error import JobError
from disco.events import Status

class RateLimitTestCase(DiscoJobTestFixture, DiscoTestCase):
    inputs = [1]

    def getdata(self, path):
        return 'badger\n' * 1000

    @staticmethod
    def map(e, params):
        msg(e)
        return []

    def runTest(self):
        self.assertRaises(JobError, self.job.wait)
        self.assertEquals(self.job.jobinfo()['active'], 'dead')

class InternalRateLimitTestCase(DiscoJobTestFixture, DiscoTestCase):
    inputs = [1]

    def getdata(self, path):
        return 'badger\n' * 1000

    @staticmethod
    def map(e, params):
        Status("Internal msg")
        return []

    def runTest(self):
        self.job.wait()
        self.assertEquals(self.job.jobinfo()['active'], 'ready')

class AnotherRateLimitTestCase(RateLimitTestCase):
    @staticmethod
    def map(e, params):
        return []

    def runTest(self):
        self.job.wait()
        self.assertEquals(self.job.jobinfo()['active'], 'ready')

class YetAnotherRateLimitTestCase(RateLimitTestCase):
    @staticmethod
    def map(e, params):
        for i in xrange(100000):
            print 'foobar'
        return []
