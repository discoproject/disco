from disco.test import DiscoJobTestFixture, DiscoTestCase

class DiscoAPITestCase(DiscoJobTestFixture, DiscoTestCase):
    input = ['raw://discoapi']

    @staticmethod
    def map(e, params):
        for i in range(3):
            msg('--special_test_string--')
        return [(e, '')]

    @property
    def answers(self):
        yield 'discoapi', ''

    def runTest(self):
        super(DiscoAPITestCase, self).runTest()
        special_messages = [message for offset, (time, node, message) in self.job.events()
                    if '--special_test_string--' in message]
        self.assertEquals(len(special_messages), 3)
