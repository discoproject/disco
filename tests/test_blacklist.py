from disco.test import DiscoJobTestFixture, DiscoTestCase
from disco.util import flatten
import thread

N = 4

class BlacklistTestCase(DiscoJobTestFixture, DiscoTestCase):
    input = []
    scheduler = {"max_cores": 1}

    def getdata(self, path):
        if path in self.whitelist:
            n, next = self.whitelist[path]
            if n == 1:
                self.disco.whitelist(next)
            self.whitelist[path] = (n - 1, next)
        return path

    def map_input_stream(stream, size, url, params):
        from disco.util import urlsplit
        from disco import comm
        scheme, netloc, path = urlsplit(url)
        # test that scheduler preserved data locality
        msg("NODE %s GOT URL %s" % (Task.netloc, url))
        assert netloc == Task.netloc
        return comm.open_remote("http://%s/%s" % (path, netloc))
    map_input_stream = [map_input_stream]

    def map(e, params):
        return [(e, '')]

    def setUp(self):
        host, port = self.test_server_address
        # assumption: scheduler starts scheduling tasks in the
        # order specified by self.input
        self.blacklisted = sorted(self.nodes.keys())
        self.input = flatten([N * ['http://%s/%s:%d' % (node, host, port)]
                for node in self.blacklisted])
        self.whitelist = {}
        for i in range(len(self.blacklisted) - 1):
            self.disco.blacklist(self.blacklisted[i + 1])
            self.whitelist[self.blacklisted[i]] =\
                (N, self.blacklisted[i + 1])
        super(BlacklistTestCase, self).setUp()

    def runTest(self):
        self.assertEquals(
            list(flatten(N  * [b] for b in self.blacklisted)),
            sorted([n for n, v in self.results]))

    def tearDown(self):
        for node in self.blacklisted:
            self.disco.whitelist(node)
        super(BlacklistTestCase, self).tearDown()

