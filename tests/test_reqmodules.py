from disco.test import DiscoJobTestFixture, DiscoTestCase

class RequiredModulesTestCase(DiscoJobTestFixture, DiscoTestCase):
    inputs = [1, 485, 3245]

    def getdata(self, path):
        return '%s\n' % path

    @staticmethod
    def map(e, params):
        k = int(math.ceil(float(e))) ** 2
        return [(base64.encodestring(str(k)), '')]

    @property
    def answers(self):
        from base64 import encodestring
        from math import ceil
        for i in self.inputs:
            yield encodestring(str(int(ceil(i)) ** 2)), ''

    def runTest(self):
        for result, answer in zip(sorted(self.results), sorted(self.answers)):
            self.assertEquals(result, answer)
