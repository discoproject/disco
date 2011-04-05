import thread

from disco.test import TestCase, TestJob, FailedReply

class TempFailJob(TestJob):
    @staticmethod
    def map(e, params):
        yield int(e) * 10, ''

class TempFailTestCase(TestCase):
    input = range(50)

    def serve(self, path):
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
        self.fail = map(str, self.input[::2])
        super(TempFailTestCase, self).setUp()

    def runTest(self):
        self.job = TempFailJob().run(input=self.test_server.urls(self.input))
        self.assertEquals(sum(k for k, v in self.results(self.job)), 122500)
