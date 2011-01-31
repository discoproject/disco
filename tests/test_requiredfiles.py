from disco.test import DiscoJobTestFixture, DiscoTestCase

import os, sys

sys.path.append(os.path.join(os.path.dirname(__file__), 'support'))

class RequiredFilesTestCase(DiscoJobTestFixture, DiscoTestCase):
    inputs = ['123']
    required_modules = ['extramodule1', 'extramodule2']

    def getdata(self, path):
        return '%s\n' % path

    @staticmethod
    def map(e, params):
        x = extramodule1.magic(int(e))
        y = extramodule2.kungfu(x)
        return [('', y)]

    @property
    def answers(self):
        yield '', int(self.inputs[0]) ** 2 + 2
