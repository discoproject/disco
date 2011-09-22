import os, struct, sys, time
from cPickle import dumps
from cStringIO import StringIO
from inspect import getmodule, getsourcefile
from zipfile import ZipFile, ZIP_DEFLATED
from zlib import compress, crc32

from disco.error import DataError

MB = 1024**2
MIN_DISK_SPACE  = 1 * MB
MAX_RECORD_SIZE = 1 * MB
HUNK_SIZE       = 1 * MB
CHUNK_SIZE      = 64 * MB

# chunk size seems to be ~ half what it should be

class Chunker(object):
    """
    chunks contain hunks
    bounds on hunk sizes do not include headers
    desired chunk size is C bytes compressed
    desired hunk size is H bytes uncompressed
    each record is at most R bytes uncompressed
    compression expands data by a factor of at most S
    each hunk is at most ((H - 1) + (R - 1)) * S bytes compressed
    a hunk will only be added to a chunk if sizeof(chunk) < C

    in the worst case for a chunk:
      sizeof(chunk) = C - 1
      a new hunk is added with size ((H - 1) + (R - 1)) * S
      sizoof(chunk) = C - 1 + ((H - 1) + (R - 1)) * S + len(header)
    """
    def __init__(self, chunk_size=CHUNK_SIZE):
        self.chunk_size = chunk_size

    def chunks(self, records):
        out = self.makeout()
        for record in records:
            if out.size > self.chunk_size:
                yield self.dumpout(out)
                out = self.makeout()
            out.append(record)
        if out.size or out.hunk_size:
            yield self.dumpout(out)

    def dumpout(self, out):
        out.close()
        return out.stream.getvalue()

    def makeout(self):
        return DiscoOutputStream(StringIO(), max_record_size=MAX_RECORD_SIZE)

class DiscoOutputStream_v0(object):
    def __init__(self, stream):
        self.stream = stream

    def add(self, k, v):
        k, v = str(k), str(v)
        self.stream.write("%d %s %d %s\n" % (len(k), k, len(v), v))

    def close(self):
        pass

class DiscoOutputStream_v1(object):
    def __init__(self, stream,
                 version=1,
                 compression_level=2,
                 min_hunk_size=HUNK_SIZE,
                 max_record_size=None):
        self.stream = stream
        self.version = version
        self.compression_level = compression_level
        self.max_record_size = max_record_size
        self.min_hunk_size = min_hunk_size
        self.size = 0
        self.hunk_size = 0
        self.hunk = StringIO()

    def add(self, k, v):
        self.append((k, v))

    def append(self, record):
        self.hunk_write(dumps(record, 1))
        if self.hunk_size > self.min_hunk_size:
            self.flush()

    def close(self):
        if self.hunk_size:
            self.flush()
        self.flush()

    def flush(self):
        hunk = self.hunk.getvalue()
        checksum = crc32(hunk) & 0xFFFFFFFF
        iscompressed = int(self.compression_level > 0)
        if iscompressed:
            hunk = compress(hunk, self.compression_level)
        data = '%s%s' % (struct.pack('<BBIQ',
                                     128 + self.version,
                                     iscompressed,
                                     checksum,
                                     len(hunk)),
                         hunk)

        self.stream.write(data)
        self.size += len(data)
        self.hunk_size = 0
        self.hunk = StringIO()

    def hunk_write(self, data):
        size = len(data)
        if self.max_record_size and size > self.max_record_size:
            raise ValueError("Record too big to write to hunk")
        self.hunk.write(data)
        self.hunk_size += size

class DiscoOutputStream(object):
    def __new__(cls, stream, version=-1, **kwargs):
        if version == 0:
            return DiscoOutputStream_v0(stream, **kwargs)
        return DiscoOutputStream_v1(stream, version=1, **kwargs)

class DiscoOutput(DiscoOutputStream_v1):
    def __init__(self, url):
        super(DiscoOutput, self).__init__(AtomicFile(url))

    def close(self):
        super(DiscoOutput, self).close()
        self.stream.close()

class DiscoZipFile(ZipFile, object):
    def __init__(self):
        self.buffer = StringIO()
        super(DiscoZipFile, self).__init__(self.buffer, 'w', ZIP_DEFLATED)

    def writepath(self, pathname, exclude=()):
        for file in files(pathname):
            name, ext = os.path.splitext(file)
            if ext not in exclude:
                self.write(file, file)

    def writemodule(self, module, arcname=None):
        if isinstance(module, basestring):
            module = __import__(module)
        self.write(getsourcefile(module), arcname=arcname)

    def writesource(self, object):
        self.writepath(getsourcefile(getmodule(object)))

    def dump(self, handle):
        handle.write(self.dumps())

    def dumps(self):
        self.buffer.reset()
        return self.buffer.read()

class NonBlockingInput(object):
    def __init__(self, file, timeout=600):
        from fcntl import fcntl, F_GETFL, F_SETFL
        self.fd, self.timeout = file.fileno(), timeout
        fcntl(self.fd, F_SETFL, fcntl(self.fd, F_GETFL) | os.O_NONBLOCK)

    def select(self, spent=0):
        from select import select
        started = time.time()
        if spent < self.timeout:
            if any(select([self.fd], [], [], self.timeout - spent)):
                return time.time() - started
        raise IOError("Reading timed out after %s seconds" % self.timeout)

    def t_read(self, nbytes, spent=0, bytes=''):
        while True:
            spent += self.select(spent)
            bytes += os.read(self.fd, nbytes - len(bytes))
            if nbytes <= len(bytes):
                return spent, bytes

    def t_read_until(self, delim, spent=0, bytes=''):
        while not bytes.endswith(delim):
            spent += self.select(spent)
            bytes += os.read(self.fd, 1)
        return spent, bytes

class AtomicFile(file):
    def __init__(self, path):
        dir = os.path.dirname(path) or '.'
        ensure_path(dir)
        ensure_free_space(dir)
        self.path = path
        self.partial = '%s.partial' % path
        self.isopen = True
        super(AtomicFile, self).__init__(self.partial, 'w')

    def close(self):
        if self.isopen:
            sync(self)
            super(AtomicFile, self).close()
            os.rename(self.partial, self.path)
            self.isopen = False

class Wait(Exception):
    """File objects can raise this if reading will block."""
    retry_after = 1
    def __init__(self, retry_after=None):
        if retry_after is not None:
            self.retry_after = retry_after

def sync(fd):
    fd.flush()
    os.fsync(fd.fileno())

def ensure_path(path):
    from errno import EEXIST
    try:
        os.makedirs(path)
    except OSError, x:
        # File exists is ok.
        # It may happen if two tasks are racing to create the directory
        if x.errno != EEXIST:
            raise

def ensure_free_space(fname):
    s = os.statvfs(fname)
    free = s.f_bsize * s.f_bavail
    if free < MIN_DISK_SPACE:
        raise DataError("Only %d KB disk space available. Task failed." % (free / 1024), fname)

def files(path):
    if os.path.isdir(path):
        for name in os.listdir(path):
            for file in files(os.path.join(path, name)):
                yield file
    else:
        yield path
