from disco.test import TestCase, TestJob

class LocalhostJob(TestJob):
    @staticmethod
    def map(e, params):
        import time
        time.sleep(0.5)
        yield int(e), ''

class LocalhostTestCase(TestCase):
    def serve(self, path):
        return path

    def setUp(self):
        self.config = self.disco.config
        nodenames   = set(name for name, workers in self.config)
        self.disco.config = [['localhost', '1']]
        super(LocalhostTestCase, self).setUp()

    def runTest(self):
        X = xrange(self.num_workers * 2)
        self.job = LocalhostJob().run(input=self.test_server.urls(X))
        self.assertEqual(sum(k for k, v in self.results(self.job)), sum(X))

    def tearDown(self):
        super(LocalhostTestCase, self).tearDown()
        self.disco.config = self.config
