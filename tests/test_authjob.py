from disco.test import TestCase, TestJob

class AuthJob(TestJob):
    @staticmethod
    def map(e, params):
        yield e.strip(), ''

class AuthTestCase(TestCase):
    def setUp(self):
        super(AuthTestCase, self).setUp()
        from cStringIO import StringIO
        self.tag = 'disco:test:authjob'
        self.ddfs.push(self.tag, [(StringIO('blobdata'), 'blob')])

    def runTest(self):
        self.ddfs.setattr(self.tag, 'ddfs:read-token', 'r')
        self.job = AuthJob().run(input=['tag://user:r@/%s' % self.tag])
        self.assertResults(self.job, [('blobdata', '')])

    def tearDown(self):
        super(AuthTestCase, self).tearDown()
        self.ddfs.delete(self.tag)
