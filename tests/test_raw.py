from disco.test import DiscoJobTestFixture, DiscoTestCase

from disco.error import JobError

class RawTestCase(DiscoJobTestFixture, DiscoTestCase):
    @property
    def input(self):
        return ['raw://eeny', 'raw://meeny', 'raw://miny', 'raw://moe']

    @staticmethod
    def map(e, params):
        return [('', e + ':map')]

    def runTest(self):
        answers, results = self.answers, list(self.results)
        self.assertEquals(len(answers), len(results))
        for key, result in results:
            self.assert_(result in answers)

    @property
    def answers(self):
        return dict((input.strip('raw://') + ':map', True)
                    for input in self.input)

class LongRawTestCase(RawTestCase):
    @property
    def input(self):
        return ['raw://%s' % ('0' * (2<<18))]

    def runTest(self):
        try:
            super(LongRawTestCase, self).runTest()
        except JobError, e:
            self.skipTest("Platform does not support long command line arguments")
