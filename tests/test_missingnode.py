from disco.test import DiscoJobTestFixture, DiscoTestCase

def unique_nodename(nodenames, count=0):
    nodename = 'missingnode_%s' % count
    if nodename not in nodenames:
        return nodename
    return unique_nodename(nodenames, count + 1)

class MissingNodeTestCase(DiscoJobTestFixture, DiscoTestCase):
    @property
    def inputs(self):
        return range(self.num_workers * 2)

    def getdata(self, path):
        return path

    @staticmethod
    def map(e, params):
        time.sleep(0.5)
        return [(int(e), '')]

    def setUp(self):
        self.config = self.disco.config
        nodenames   = set(name for name, workers in self.config)
        self.disco.config = self.config + [[unique_nodename(nodenames), '1']]
        super(MissingNodeTestCase, self).setUp()

    def runTest(self):
        self.assertEquals(sum(xrange(self.num_workers * 2)),
                  sum(int(k) for k, v in self.results))

    def tearDown(self):
        super(MissingNodeTestCase, self).tearDown()
        self.disco.config = self.config
