
import sys, time, os, cPickle, struct, zlib
import errno, fcntl
from cStringIO import StringIO

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
                yield out.dumps()
                out = self.makeout()
            out.append(record)
        if out.hunk_size:
            yield out.dumps()

    def makeout(self):
        return DiscoOutput(StringIO(), max_record_size=MAX_RECORD_SIZE)

class DiscoOutput_v0(object):
    def __init__(self, stream):
        self.stream = stream

    def add(self, k, v):
        k, v = str(k), str(v)
        self.stream.write("%d %s %d %s\n" % (len(k), k, len(v), v))

    def close(self):
        pass

    def write(self, data):
        self.stream.write(data)

class DiscoOutput_v1(object):
    def __init__(self, stream,
                 version=None,
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
        self.writer = False

    def add(self, k, v):
        self.append((k, v))

    def append(self, record):
        self.hunk_write(cPickle.dumps(record, 1))
        if self.hunk_size > self.min_hunk_size:
            self.flush()

    def close(self):
        if not self.writer:
            if self.hunk_size:
                self.flush()
            self.flush()

    def dumps(self):
        self.close()
        return self.stream.getvalue()

    def flush(self):
        hunk = self.hunk.getvalue()
        checksum = zlib.crc32(hunk) & 0xFFFFFFFF
        iscompressed = int(self.compression_level > 0)
        if iscompressed:
            hunk = zlib.compress(hunk, self.compression_level)
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
            raise ValueError("Record too big to write to hunk: %s" % record)
        self.hunk.write(data)
        self.hunk_size += size

    def write(self, data):
        self.writer = True
        self.stream.write(data)

class DiscoOutput(object):
    def __new__(cls, stream, version=-1, **kwargs):
        if version == 0:
            return DiscoOutput_v0(stream, **kwargs)
        return DiscoOutput_v1(stream, version=1, **kwargs)

class AtomicFile(file):
    def __init__(self, path, *args, **kwargs):
        dir = os.path.dirname(path)
        ensure_path(dir)
        ensure_free_space(dir)
        self.path = path
        self.partial = '%s.partial' % path
        self.isopen = True
        super(AtomicFile, self).__init__(self.partial, *args, **kwargs)

    def close(self):
        if self.isopen:
            sync(self)
            super(AtomicFile, self).close()
            os.rename(self.partial, self.path)
            self.isopen = False

def sync(fd):
    fd.flush()
    os.fsync(fd.fileno())

def ensure_path(path):
    try:
        os.makedirs(path)
    except OSError, x:
        # File exists is ok.
        # It may happen if two tasks are racing to create the directory
        if x.errno != errno.EEXIST:
            raise

def ensure_file(fname, data = None, timeout = 60, mode = 500):
    while timeout > 0:
        if os.path.exists(fname):
            return False
        try:
            fd = os.open(fname + ".partial",
                os.O_CREAT | os.O_EXCL | os.O_WRONLY, mode)
            if callable(data):
                data = data()
            os.write(fd, data)
            os.close(fd)
            os.rename(fname + ".partial", fname)
            return True
        except OSError, x:
            if x.errno == errno.EEXIST:
                time.sleep(1)
                timeout -= 1
            else:
                raise DataError("Writing external file failed", fname)
    raise DataError("Timeout in writing external file", fname)

def write_files(files, path):
    if files:
        path = os.path.abspath(path)
        ensure_path(path)
    for fname, data in files.iteritems():
        # make sure that no files are written outside the given path
        p = os.path.abspath(os.path.join(path, fname))
        if os.path.dirname(p) == path:
            ensure_file(path + "/" + fname, data=data)
        else:
            raise ValueError("Unsafe filename %s" % fname)

def ensure_free_space(fname):
    s = os.statvfs(fname)
    free = s.f_bsize * s.f_bavail
    if free < MIN_DISK_SPACE:
        raise DataError("Only %d KB disk space available. Task failed." % (free / 1024), fname)
