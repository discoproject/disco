import thread
from disco.test import DiscoJobTestFixture, DiscoTestCase, FailedReply

class TempFailTestCase(DiscoJobTestFixture, DiscoTestCase):
    inputs = range(50)

    def getdata(self, path):
        self.lock.acquire()
        if path in self.fail:
            self.fail.remove(path)
            self.lock.release()
            raise FailedReply()
        else:
            self.lock.release()
            return '%s\n' % (int(path) * 10)

    def setUp(self):
        self.lock = thread.allocate_lock()
        self.fail = map(str, TempFailTestCase.inputs[::2])
        super(TempFailTestCase, self).setUp()

    @staticmethod
    def map(e, params):
        return [(int(e) * 10, '')]

    def runTest(self):
        if len(self.nodes) > 1:
            return self.assertEquals(sum(int(k) for k, v in self.results), 122500)
        self.skipTest("Cannot test temporary node failure with < 2 nodes")
