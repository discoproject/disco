
import sys, time, os, cPickle, cStringIO, struct, zlib
import errno, fcntl

from disco.error import DataError

MIN_DISK_SPACE = 1024**2

class DiscoOutput(object):
    VERSION = 1
    def __init__(self, stream, compress_level, min_chunk, version):
        self.compress_level = compress_level
        self.min_chunk = min_chunk
        self.version = self.VERSION if version < 0 else version
        self.stream = stream
        self.chunk = cStringIO.StringIO()
        self.chunk_size = 0
        self.newstream = False

    def write(self, buf):
        self.stream.write(buf)

    def dump(self):
        encoded_data = self.chunk.getvalue()
        checksum = zlib.crc32(encoded_data) & 0xFFFFFFFF
        if self.compress_level:
            encoded_data = zlib.compress(encoded_data, self.compress_level)
        self.stream.write(struct.pack('<BBIQ',
                          128 + self.VERSION,
                          int(self.compress_level > 0),
                          checksum,
                          len(encoded_data)) + encoded_data)
        self.chunk = cStringIO.StringIO()
        self.chunk_size = 0

    def add(self, k, v):
        if self.version == 0:
            k = str(k)
            v = str(v)
            self.stream.write("%d %s %d %s\n" % (len(k), k, len(v), v))
            return
        self.newstream = True
        buf = cPickle.dumps((k, v), 1)
        self.chunk_size += len(buf)
        self.chunk.write(buf)
        if self.chunk_size > self.min_chunk:
            self.dump()

    def close(self):
        if self.newstream:
            if self.chunk_size:
                self.dump()
            self.dump()

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
            super(AtomicFile, self).close()
            os.rename(self.partial, self.path)
            self.isopen = False

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
