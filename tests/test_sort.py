import base64, string
from disco.test import DiscoJobTestFixture, DiscoTestCase

class SortTestCase(DiscoJobTestFixture, DiscoTestCase):
    inputs     = [''] * 100
    sort       = True

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
        for k, vs in disco.util.kvgroup(iter):
            assert k not in d, "Key %s seen already, sorting failed." % k
            d.add(k)
            out.add(base64.decodestring(k), len(list(vs)))

    @property
    def answers(self):
        return sorted((str(k), 1000) for k in list(string.ascii_lowercase) + range(10))

class MergeSortTestCase(SortTestCase):
    partitions = 0
    sort       = 'merge'

    @staticmethod
    def map(e, params):
        letters = list(string.ascii_lowercase * 10)
        numbers = [str(i) for i in range(10)] * 10
        for char in sorted(letters + numbers):
            yield base64.encodestring(char), ''
