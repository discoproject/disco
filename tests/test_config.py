import time

from disco.test import TestCase, TestJob
from disco.util import shuffled
from disco.compat import str_to_bytes

def unique_nodename(nodenames, count=0):
    nodename = 'missingnode_{0}'.format(count)
    if nodename not in nodenames:
        return nodename
    return unique_nodename(nodenames, count + 1)

class ConfigJob(TestJob):
    @staticmethod
    def map(e, params):
        time.sleep(0.5)
        yield e, ''

class ConfigTestCase(TestCase):
    def checkAnswers(self, job, input):
        self.assertEquals(sorted(self.results(job)),
                          sorted((str_to_bytes(str(i)), '') for i in input))

    def configTest(self, config):
        input = range(self.num_workers * 2)
        self.disco.config = config
        self.job = ConfigJob().run(input=self.test_server.urls(input))
        self.checkAnswers(self.job, input)

    def serve(self, path):
        return path

    def setUp(self):
        super(ConfigTestCase, self).setUp()
        self.config = self.disco.config

    def test_changes(self):
        if len(self.nodes) < 2:
            self.skipTest("Cannot test node changes with < 2 nodes")
        else:
            local = ['url://{0}'.format(node)
                     for node, max_workers in self.nodes.items()
                     for x in range(max_workers * 2)]
            input = shuffled(local + list(range(self.num_workers)))
            self.job = ConfigJob().run(input=self.test_server.urls(input))
            time.sleep(5)
            self.disco.config = self.config[:2]
            time.sleep(5)
            self.disco.config = self.config[:1]
            time.sleep(5)
            self.disco.config = self.config
            time.sleep(3)
            self.checkAnswers(self.job, input)

    def test_missing_node(self):
        nodenames = set(name for name, workers in self.config)
        self.configTest(self.config + [[unique_nodename(nodenames), '1']])

    def tearDown(self):
        super(ConfigTestCase, self).tearDown()
        self.disco.config = self.config
