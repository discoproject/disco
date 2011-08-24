from itertools import count
from cPickle import dumps
from unittest import TestCase
from disco.fileutils import Chunker, CHUNK_SIZE, HUNK_SIZE

def _records():
    for v in count():
        yield (v, v)

def _sizelimitedinput(max_size):
    size = 0
    for v in count():
        record = (v, v)
        yield record
        # use same serializer than DiscoOutputStream
        size += len(dumps(record, 1))
        if size >= max_size:
            break


class ChunkerTestCase(TestCase):

    def test_exactly_a_hunk(self):
        records = _sizelimitedinput(HUNK_SIZE)
        chunker = Chunker(chunk_size=HUNK_SIZE * 2)
        chunks = list(chunker.chunks(records))
        self.assertEqual(len(chunks), 1)

    def test_less_than_a_hunk(self):
        records = _sizelimitedinput(HUNK_SIZE / 2)
        chunker = Chunker(chunk_size=HUNK_SIZE * 2)
        chunks = list(chunker.chunks(records))
        self.assertEqual(len(chunks), 1)

    def test_more_than_a_hunk(self):
        chunk_size = HUNK_SIZE * 2
        records = _sizelimitedinput(chunk_size)
        chunker = Chunker(chunk_size=chunk_size)
        chunks = list(chunker.chunks(records))
        self.assertEqual(len(chunks), 1)

