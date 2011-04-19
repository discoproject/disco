from disco.test import TestCase, TestJob

class RawJob(TestJob):
    @staticmethod
    def map(e, params):
        yield 'raw://%s' % e, ''

class RawTestCase(TestCase):
    def runTest(self):
        input = ['raw://eeny', 'raw://meeny', 'raw://miny', 'raw://moe']
        self.job = RawJob().run(input=input)
        self.assertEqual(sorted(self.results(self.job)),
                         sorted((i, '') for i in input))

