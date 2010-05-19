import random, time, sys
from disco.test import DiscoJobTestFixture, DiscoTestCase

class NodeChangesTestCase(DiscoJobTestFixture, DiscoTestCase):

    @property
    def input(self):
        local = ['url://%s' % node
                    for node, max_workers in self.real_nodes.iteritems()
                        for x in xrange(max_workers * 2)]
        host, port = self.test_server_address
        inputs = local + ["http://%s:%s/%d" % (host, port, i)
                            for i in range(len(local) / 2)]
        random.shuffle(inputs)
        return inputs

    def getdata(self, path):
        return path

    @staticmethod
    def map(e, params):
        time.sleep(10)
        return [(e, '')]

    def setUp(self):
        self.orig_config = self.disco.config
        self.real_nodes = self.nodes
        self.config = [[node, str(max_workers)]
                        for node, max_workers in self.nodes.iteritems()]
        self.disco.config = self.config
        super(NodeChangesTestCase, self).setUp()

    def runTest(self):
        if len(self.nodes) < 2:
            self.skipTest("Cannot test node changes with < 2 nodes")
        else:
            time.sleep(5)
            self.disco.config = self.config[:2]
            time.sleep(5)
            self.disco.config = [self.config[0]]
            time.sleep(5)
            self.disco.config = self.config
            self.assertEquals(sorted(self.results), self.answers)

    @property
    def answers(self):
        local = [(url, '') for url in self.input if url.startswith('url://')]
        return sorted(local + [(str(i), '') for i in range(len(local) / 2)])

    def tearDown(self):
        super(NodeChangesTestCase, self).tearDown()
        self.disco.config = self.orig_config
