from disco.test import TestCase, TestJob
from disco.worker.classic.modutil import find_modules, ModUtilImportError

import sys, os

support = os.path.join(os.path.dirname(__file__), 'support')
mod1req = ('mod1', os.path.normpath(os.path.join(support, 'mod1.py')))
mod2req = ('mod2', os.path.normpath(os.path.join(support, 'mod2.py')))

def system_modules():
    os.path.abspath('')
    random.random()
    time.time()

def local_module():
    extramodule1.magic(5)

def missing_module():
    missing_module_ifitexiststhistestwillfail.magic()

def recursive_module():
    mod1.plusceil(1, 2)

class ModUtilJob(TestJob):
    @staticmethod
    def map(e, params):
        x, y = [float(x) for x in e.split('|')]
        yield mod1.plusceil(x, y) + math.ceil(1.5), ''

class RequiredFilesJob(TestJob):
    @staticmethod
    def map(e, params):
        x = extramodule1.magic(int(e))
        y = extramodule2.kungfu(x)
        open('lib/mod1.py')
        import mod1
        yield '', y

class ModUtilTestCase(TestCase):
    def serve(self, path):
        return '%s\n' % path

    def setUp(self):
        super(ModUtilTestCase, self).setUp()
        self.sys_path = sys.path
        sys.path.append(support)
        python_path = os.getenv('PYTHONPATH', '').split(':')
        os.environ['PYTHONPATH'] = ':'.join(python_path + [support])

    def tearDown(self):
        super(ModUtilTestCase, self).tearDown()
        sys.path = self.sys_path

    def assertFindsModules(self, functions, modules, send_modules=False, recurse=True):
        self.assertEquals(sorted(find_modules(functions,
                                              send_modules=send_modules,
                                              recurse=recurse)),
                          sorted(modules))

    def test_system(self):
        self.assertFindsModules([system_modules], ['os', 'random', 'time'])

    def test_local(self):
        self.assertFindsModules([local_module], ['extramodule1'])

    def test_missing(self):
        self.assertRaises(ModUtilImportError, lambda: find_modules([missing_module]))

    def test_recursive(self):
        self.assertFindsModules([recursive_module], [mod1req, mod2req], send_modules=True)

    def test_norecursive(self):
        self.assertFindsModules([recursive_module], ['mod1'], recurse=False)

    def test_auto_modules(self):
        self.job = ModUtilJob().run(input=self.test_server.urls(['0.5|1.2']))
        self.assertResults(self.job, [(4.0, '')])

    def test_find_modules(self):
        self.job = ModUtilJob()
        self.job.run(input=self.test_server.urls(['0.5|1.2']),
                     required_modules=find_modules([self.job.map]))
        self.assertResults(self.job, [(4.0, '')])

    def test_list_modules(self):
        self.job = ModUtilJob().run(input=self.test_server.urls(['0.5|1.2']),
                                    required_modules=['math', mod1req, mod2req])
        self.assertResults(self.job, [(4.0, '')])

    def test_required_files(self):
        self.job = RequiredFilesJob().run(input=self.test_server.urls([123]),
                                          required_files=[mod1req[1], mod2req[1]])
        self.assertResults(self.job, [('', 123 ** 2 + 2)])
