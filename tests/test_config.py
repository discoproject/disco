from disco.test import TestCase, TestJob

def unique_nodename(nodenames, count=0):
    nodename = 'missingnode_%s' % count
    if nodename not in nodenames:
        return nodename
    return unique_nodename(nodenames, count + 1)

class ConfigJob(TestJob):
    @staticmethod
    def map(e, params):
        import time
        time.sleep(0.5)
        yield int(e), ''

class LocalhostTestCase(TestCase):
    def config(self, _config):
        return [['localhost', '1']]

    def serve(self, path):
        return path

    def setUp(self):
        super(LocalhostTestCase, self).setUp()
        self._config = self.disco.config
        self.disco.config = self.config(self._config)

    def runTest(self):
        X = xrange(self.num_workers * 2)
        self.job = ConfigJob().run(input=self.test_server.urls(X))
        self.assertEqual(sum(k for k, v in self.results(self.job)), sum(X))

    def tearDown(self):
        super(LocalhostTestCase, self).tearDown()
        self.disco.config = self._config

class MissingNodeTestCase(LocalhostTestCase):
    def config(self, _config):
        nodenames = set(name for name, workers in _config)
        return _config + [[unique_nodename(nodenames), '1']]
