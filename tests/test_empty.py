from disco.test import DiscoJobTestFixture, DiscoTestCase

class EmptyTestCase(DiscoJobTestFixture, DiscoTestCase):
    def runTest(self):
        self.assertEquals(list(self.results), [])
