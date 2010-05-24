import base64, string
from disco.test import DiscoJobTestFixture, DiscoTestCase

class MemorySortTestCase(DiscoJobTestFixture, DiscoTestCase):
    inputs     = [''] * 100
    sort       = True
    mem_sort_limit = 1024 ** 4

    def getdata(self, path):
        return '\n'

    @staticmethod
    def map(e, params):
        letters  = sorted(string.ascii_lowercase * 10)
        numbers  = [str(i) for i in range(10)] * 10
        sequence = [(base64.encodestring(s), '') for s in letters + numbers]
        random.shuffle(sequence)
        return sequence

    @staticmethod
    def reduce(iter, out, params):
        d = set()
        for k, v in itertools.groupby(iter, key=lambda x: x[0]):
            assert k not in d, "Key %s seen already, sorting failed." % k
            d.add(k)
            out.add(k, len(list(v)))

    @property
    def answers(self):
        return dict((str(k), 1000) for k in list(string.ascii_lowercase) + range(10))

    def runTest(self):
        answers = self.answers
        for key, value in self.results:
            self.assertEquals(value, answers.pop(base64.decodestring(key)))
        self.assertEquals(answers, {})

class ExternalSortTestCase(MemorySortTestCase):
    mem_sort_limit = 0







