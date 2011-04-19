import time, cStringIO
from random import randint, choice
from string import ascii_lowercase
from disco.error import DataError
from disco.fileutils import DiscoOutputStream
from disco.test import TestCase
from disco.worker.classic.func import disco_input_stream

class DiscoProtoTestCase(TestCase):

    def setUp(self):
        super(DiscoProtoTestCase, self).setUp()
        self.data = [(randint(0, 100) * choice(ascii_lowercase), randint(0, 1e6))
                     for i in range(1000000)] + [('', '')]
        self.size = sum(len(k) + len(str(v)) for k, v in self.data) / 1024**2

    def encode(self, stream, data):
        t = time.time()
        for k, v in data:
            stream.add(k, v)
        stream.close()
        return time.time() - t

    def decode(self, fd, size, url, ignore_corrupt):
        t = time.time()
        res = list(disco_input_stream(fd, size, url,
                                      ignore_corrupt=ignore_corrupt))
        return time.time() - t, res

    def codec(self,
              version=1,
              corrupt=False,
              ignore_corrupt=False,
              **kwargs):
        buf = cStringIO.StringIO()
        stream = DiscoOutputStream(buf, version=version, **kwargs)
        t = self.encode(stream, self.data)
        final_size = len(buf.getvalue())
        final_mb = final_size / 1024**2
        msg = "%1.2fMB encoded in %1.3fs (%1.2fMB/s), "\
              "encoded size %1.3fMB (version: %d, %s)" %\
                    (self.size, t, self.size / t, final_mb, version, kwargs)
        if corrupt:
            buf.seek(0)
            new = cStringIO.StringIO()
            new.write(buf.read(100))
            new.write('X')
            buf.read(1)
            new.write(buf.read())
            buf = new

        buf.seek(0)
        t, res = self.decode(buf, final_size, "nourl",
                             ignore_corrupt=ignore_corrupt)
        if not ignore_corrupt:
            print "%s, decoded in %1.3fs (%1.2fMB/s)" % (msg, t, self.size / t)
        return res

    def test_compress(self):
        self.assertEquals(self.codec(compression_level=2), self.data)

    def test_nocompress(self):
        self.assertEquals(self.codec(compression_level=0), self.data)

    def test_oldformat(self):
        data = [(str(k), str(v)) for k, v in self.data]
        self.assertEquals(self.codec(version=0), data)

    def test_corrupt(self):
        fn = lambda: self.codec(compression_level=2, corrupt=True)
        self.assertRaises(DataError, fn)

    def test_ignorecorrupt(self):
        self.codec(compression_level=2, corrupt=True, ignore_corrupt=True)
