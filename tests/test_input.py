from disco.test import DiscoJobTestFixture, DiscoTestCase

class EmptyInputTestCase(DiscoJobTestFixture, DiscoTestCase):
    input = []

    @staticmethod
    def map(e, params):
        return []

    @property
    def answers(self):
        return []
