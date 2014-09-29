from disco.test import TestCase
from time import sleep

from disco.ddfs import DDFS
from disco.comm import download

from uuid import uuid1
from functools import partial

FILE= "/tmp/hello"
PREFIX = "disco:test:gc:_"
COUNT = 100

def wait_for_gc_to_finish(d):
    for i in range(100):
        response = d._download(d.master + "/ddfs/ctrl/gc_status")
        if response != "":
            sleep(1)
    if response != "":
        print "GC did not finish in 100 seconds: ", response

class DdfsGcTests(TestCase):
    def setUp(self):
        self.d = DDFS()
        wait_for_gc_to_finish(self.d)
        with open(FILE, 'w') as f:
            print >>f, "hello world!"

    def _test_push(self, prefix, func):
        for i in range(COUNT):
            func(prefix + str(i), [FILE])
        self.d._download(self.d.master + "/ddfs/ctrl/gc_start")

        wait_for_gc_to_finish(self.d)
        for i in range(COUNT):
            blobs = [b for b in self.d.blobs(prefix + str(i))]
            self.assertEquals(len(blobs), 1)
            self.assertGreater(len(blobs[0]), 0)

    def test_push_deterministic(self):
        self._test_push(PREFIX + str(uuid1()), self.d.push)

    def test_push_same_tag(self):
        self._test_push(PREFIX, self.d.push)

    def test_chunk_deterministic(self):
        self._test_push(PREFIX + str(uuid1()), self.d.chunk)

    def test_chunk_same_tag(self):
        self._test_push(PREFIX, self.d.chunk)

    def test_chunk_delayed(self):
        self._test_push(PREFIX, partial(self.d.chunk, delayed=True))

    def test_push_delayed(self):
        self._test_push(PREFIX, partial(self.d.push, delayed=True))

    def test_chunk_none_replicas(self):
        self._test_push(PREFIX, partial(self.d.chunk, replicas=None))

    def _test_func_tag(self, prefix, func):
        def chunk_tag(name, input):
            _, blob_set = func(name, input)
            self.d.tag(name + "tag", blob_set)
        self._test_push(PREFIX, chunk_tag)

        for i in range(COUNT):
            blobs = [b for b in self.d.blobs(prefix + str(i) + "tag")]
            self.assertEquals(len(blobs), 1)
            self.assertGreater(len(blobs[0]), 0)

    def test_chunk_tag(self):
        self._test_func_tag(PREFIX, self.d.chunk)

    def test_chunk_tag_delayed(self):
        self._test_func_tag(PREFIX, partial(self.d.chunk, delayed=True))

    def test_push_tag(self):
        self._test_func_tag(PREFIX, self.d.push)

    def test_push_tag_delayed(self):
        self._test_func_tag(PREFIX, partial(self.d.push, delayed=True))

    def tearDown(self):
        tags = self.d.list(PREFIX)
        for tag in tags:
            self.d.delete(tag)
