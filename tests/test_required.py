import os, sys

from disco.test import TestCase, TestJob

class RequiredFilesJob(TestJob):
    @staticmethod
    def map(e, params):
        x = extramodule1.magic(int(e))
        y = extramodule2.kungfu(x)
        yield '', y

class RequiredModulesJob(TestJob):
    @staticmethod
    def map(e, params):
        k = int(math.ceil(float(e))) ** 2
        yield base64.encodestring(str(k)), ''

class RequiredTestCase(TestCase):
    def serve(self, path):
        return '%s\n' % path

    def setUp(self):
        super(RequiredTestCase, self).setUp()
        self.sys_path = sys.path
        self.support  = os.path.join(os.path.dirname(__file__), 'support')
        sys.path.append(self.support)
        os.environ['PYTHONPATH'] += ':%s' % self.support

    def tearDown(self):
        super(RequiredTestCase, self).tearDown()
        sys.path = self.sys_path

    def test_required_files(self):
        sys.path.append(os.path.join(os.path.dirname(__file__), 'support'))
        self.job = RequiredFilesJob().run(input=self.test_server.urls([123]))
        self.assertResults(self.job, [('', 123 ** 2 + 2)])

    def test_required_modules(self):
        from base64 import encodestring
        from math import ceil
        input = [1, 485, 3245]
        self.job = RequiredModulesJob().run(input=self.test_server.urls(input))
        answers = [(encodestring(str(int(ceil(i)) ** 2)), '') for i in input]
        self.assertAllEqual(sorted(self.results(self.job)), sorted(answers))
