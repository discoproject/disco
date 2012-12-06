from disco.test import TestCase, TestJob

class AuthJob(TestJob):
    @staticmethod
    def map(e, params):
        yield e.strip(), ''

class AuthTestCase(TestCase):
    def setUp(self):
        super(AuthTestCase, self).setUp()
        from disco.compat import BytesIO
        self.tag = 'disco:test:authjob'
        self.ddfs.push(self.tag, [(BytesIO(b'blobdata'), 'blob')])

    def runTest(self):
        self.ddfs.setattr(self.tag, 'ddfs:read-token', 'r')
        self.job = AuthJob().run(input=['tag://user:r@/{0}'.format(self.tag)])
        self.assertResults(self.job, [(b'blobdata', '')])

    def tearDown(self):
        super(AuthTestCase, self).tearDown()
        self.ddfs.delete(self.tag)
