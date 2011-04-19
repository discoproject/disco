from disco.test import TestCase, TestJob

class InitJob(TestJob):
    params = {'x': 10}
    sort = False

    @staticmethod
    def map_init(iter, params):
        iter.next()
        params['x'] += 100

    @staticmethod
    def map(e, params):
        yield e, int(e) + params['x']

    @staticmethod
    def reduce_init(iter, params):
        params['y'] = 1000

    @staticmethod
    def reduce(iter, params):
        for k, v in iter:
            yield k, int(v) + params['y']

class InitTestCase(TestCase):
    def serve(self, path):
        return 'skipthis\n' + ('%s\n' % path) * 10

    def runTest(self):
        self.job = InitJob().run(input=self.test_server.urls(range(10)))
        results = list(self.results(self.job))
        for k, v in results:
            self.assertEquals(int(k) + 1110, int(v))
        self.assertEquals(len(results), 100)
