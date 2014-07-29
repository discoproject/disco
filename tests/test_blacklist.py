from disco.test import TestCase, TestJob
from disco.util import chainify, urlsplit
from disco.comm import open_remote
from disco.compat import bytes_to_str

N = 4

class BlacklistJob(TestJob):
    scheduler = {'max_cores': 1}
    def map_input_stream(stream, size, url, params):
        scheme, (host, port), test_server = urlsplit(url)
        # test that scheduler observed the blacklist
        print("NODE {0} GOT URL {1}".format(Task.host, url))
        assert Task.host <= host
        return open_remote("http://{0}/{1}".format(test_server, host))
    map_input_stream = [map_input_stream]

    @staticmethod
    def map(e, params):
        yield bytes_to_str(e), ''

class BlacklistTestCase(TestCase):
    def serve(self, path):
        if path in self.whitelist:
            n, next = self.whitelist[path]
            if n == 1:
                self.disco.whitelist(next)
            self.whitelist[path] = (n - 1, next)
        return path

    def setUp(self):
        super(BlacklistTestCase, self).setUp()
        self.blacklist = sorted(self.nodes)
        self.whitelist = {}
        for i in range(len(self.blacklist) - 1):
            self.disco.blacklist(self.blacklist[i + 1])
            self.whitelist[self.blacklist[i]] = (N, self.blacklist[i + 1])

    def runTest(self):
        host, port = self.test_server_address
        input = chainify(['http://{0}/{1}:{2}'.format(node, host, port)] * N
                         for node in self.blacklist)
        self.job = BlacklistJob().run(input=input)
        self.assertAllEqual(sorted(k for k, v in self.results(self.job)),
                            chainify([node] * N for node in self.blacklist))

    def tearDown(self):
        super(BlacklistTestCase, self).tearDown()
        for node in self.blacklist:
            self.disco.whitelist(node)
