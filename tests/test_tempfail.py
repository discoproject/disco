from disco.test import DiscoJobTestFixture, DiscoTestCase, FailedReply

class TempFailTestCase(DiscoJobTestFixture, DiscoTestCase):
        inputs = xrange(10)

        def getdata(self, path):
                from datetime import datetime
                if datetime.now().microsecond % 2:
                        raise FailedReply()
                return '%s\n' % (int(path) * 10)

        @staticmethod
        def map(e, params):
                return [(int(e) * 10, '')]

        def runTest(self):
                self.assertEquals(sum(int(k) for k, v in self.results), 4500)
