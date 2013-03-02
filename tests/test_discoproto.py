import time
from random import randint, choice
from string import ascii_lowercase
from disco.compat import BytesIO
from disco.error import DataError
from disco.fileutils import DiscoOutputStream
from disco.test import TestCase
from disco.worker.task_io import disco_input_stream

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
        buf = BytesIO()
        stream = DiscoOutputStream(buf, version=version, **kwargs)
        t = self.encode(stream, self.data)
        final_size = len(buf.getvalue())
        final_mb = final_size / 1024**2
        msg = (("{0:1.2f}MB encoded in {1:1.3f}s ({2:1.2f}MB/s), "
                "encoded size {3:1.3f}MB (version: {4}, {5})")
               .format(self.size, t, self.size / t, final_mb, version, kwargs))
        if corrupt:
            buf.seek(0)
            new = BytesIO()
            new.write(buf.read(100))
            new.write(b'X')
            buf.read(1)
            new.write(buf.read())
            buf = new

        buf.seek(0)
        t, res = self.decode(buf, final_size, "nourl",
                             ignore_corrupt=ignore_corrupt)
        if not ignore_corrupt:
            print("{0}, decoded in {1:1.3f}s ({2:1.2f}MB/s)"
                  .format(msg, t, self.size / t))
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
