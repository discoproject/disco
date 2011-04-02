import cPickle, errno, struct, os, sys, time, zlib
from cStringIO import StringIO
from zipfile import ZipFile, ZIP_DEFLATED

from disco.error import DataError
from disco.util import modulify

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

    @property
    def path(self):
        return self.stream.path

    def add(self, k, v):
        k, v = str(k), str(v)
        self.stream.write("%d %s %d %s\n" % (len(k), k, len(v), v))

    def close(self):
        pass

    def write(self, data):
        self.stream.write(data)

class DiscoOutput_v1(DiscoOutput_v0):
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

    def add(self, k, v):
        self.append((k, v))

    def append(self, record):
        self.hunk_write(cPickle.dumps(record, 1))
        if self.hunk_size > self.min_hunk_size:
            self.flush()

    def close(self):
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
            raise ValueError("Record too big to write to hunk")
        self.hunk.write(data)
        self.hunk_size += size

class DiscoOutput(object):
    def __new__(cls, stream, version=-1, **kwargs):
        if version == 0:
            return DiscoOutput_v0(stream, **kwargs)
        return DiscoOutput_v1(stream, version=1, **kwargs)

class DiscoZipFile(ZipFile, object):
    def __init__(self):
        self.buffer = StringIO()
        super(DiscoZipFile, self).__init__(self.buffer, 'w', ZIP_DEFLATED)

    def modulepath(self, module, basename='lib'):
        module = modulify(module)
        name, ext = os.path.splitext(module.__file__)
        modpath = '%s%s' % (os.path.join(*module.__name__.split('.')), ext)
        return os.path.join(basename, modpath)

    def writebytes(self, pathname, bytes, basename=''):
        self.writestr(os.path.join(basename, pathname.strip('/')), bytes)

    def writemodule(self, module, basename='lib'):
        module = modulify(module)
        self.write(module.__file__, self.modulepath(module, basename=basename))

    def writepath(self, pathname, basename=''):
        for file in files(pathname):
            self.write(file, os.path.join(basename, file.strip('/')))

    def writepy(self, pathname, basename='lib'):
        if ispackage(pathname):
            basename = os.path.join(basename, os.path.basename(pathname))
        for module in modules(pathname):
            self.write(os.path.join(pathname, module),
                       os.path.join(basename, module))

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

    def readline(self):
        spent, line = self.select(), os.read(self.fd, 1)
        while not line.endswith('\n'):
            spent += self.select(spent)
            line += os.read(self.fd, 1)
        return line

    def read(self, bytes=-1):
        spent = self.select()
        read = os.read(self.fd, bytes)
        while bytes > 0 and len(read) < bytes:
            spent += self.select(spent)
            read += os.read(self.fd, bytes - len(read))
        return read

    def select(self, spent=0):
        from select import select
        started = time.time()
        if spent < self.timeout:
            if any(select([self.fd], [], [], self.timeout - spent)):
                return time.time() - started
        raise IOError("Reading timed out after %s seconds" % self.timeout)

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

def ensure_free_space(fname):
    s = os.statvfs(fname)
    free = s.f_bsize * s.f_bavail
    if free < MIN_DISK_SPACE:
        raise DataError("Only %d KB disk space available. Task failed." % (free / 1024), fname)

def ismodule(path):
    if os.path.isfile(path):
        name, ext = os.path.splitext(path)
        return ext in ('.py', '.so')

def ispackage(path):
    return os.path.isdir(path) and '__init__.py' in os.listdir(path)

def files(path):
    if os.path.isdir(path):
        for name in os.listdir(path):
            for file in files(os.path.join(path, name)):
                yield file
    else:
        yield path

def modules(dirpath):
    for name in os.listdir(dirpath):
        path = os.path.join(dirpath, name)
        if ismodule(path):
            yield name
        elif ispackage(path):
            for submodule in modules(path):
                yield '%s/%s' % (name, submodule)
