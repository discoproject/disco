import sys
from os.path import dirname, join

from disco.job import JobChain
from disco.test import TestCase

for dir in ('util', 'faq'):
    sys.path.insert(0, join(dirname(dirname(__file__)), 'examples', dir))

from chain import FirstJob, ChainJob
from chunk import LineChunker
from grep import Grep
from wordcount import WordCount
from wordcount_ddb import WordCount as WordCountDDB
from query_ddb import Query

chekhov = 'http://discoproject.org/media/text/chekhov.txt'

class ExamplesTestCase(TestCase):
    def test_chain(self):
        a, b = FirstJob(), ChainJob()
        self.job = JobChain({a: ['raw://0'],
                             b: a}).wait()
        self.assertResults(b, [(2, '')])

    def test_chunk(self):
        self.tag = 'disco:test:examples:chunks'
        self.job = LineChunker().run(input=[chekhov], params=self.tag)
        self.assertEquals(len(list(self.results(self.job))), 1)

    def test_discodb(self):
        if self.settings['DISCO_TEST_DISCODB']:
            a, b = WordCountDDB(), Query()
            b.params = 'discover'
            self.job = JobChain({a: [chekhov], b: a}).wait()
            self.assertEquals(self.results(b).next()[1], ['2'])
        else:
            self.skipTest("DISCO_TEST_DISCODB not set")

    def test_grep(self):
        self.job = Grep().run(input=[chekhov], params='d.*?co')
        self.assertEquals(len(list(self.results(self.job))), 17)

    def test_wordcount(self):
        self.job = WordCount().run(input=[chekhov])
        self.assertEquals(len(list(self.results(self.job))), 12339)

    def tearDown(self):
        super(ExamplesTestCase, self).tearDown()
        if hasattr(self, 'tag'):
            self.ddfs.delete(self.tag)
