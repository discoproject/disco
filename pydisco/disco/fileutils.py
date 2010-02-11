
import sys, time, os
import errno, fcntl

from disco.util import data_err, err

class AtomicFile(file):
    def __init__(self, fname, *args, **kw):
        ensure_path(os.path.dirname(fname))
        self.fname = fname
        self.isopen = True
        super(AtomicFile, self).__init__(
            fname + ".partial", *args, **kw)

    def close(self):
        if self.isopen:
            super(AtomicFile, self).close()
            os.rename(self.fname + ".partial", self.fname)
            self.isopen = False

class PartitionFile(AtomicFile):
    def __init__(self, partfile, tmpname, *args, **kw):
        self.partfile = partfile
        self.tmpname = tmpname
        self.isopen = True
        super(PartitionFile, self).__init__(
            tmpname, *args, **kw)

    def close(self):
        if self.isopen:
            super(PartitionFile, self).close()
            safe_append(file(self.tmpname), self.partfile)
            os.remove(self.tmpname)
            self.isopen = False

def ensure_path(path):
    try:
        os.makedirs(path)
    except OSError, x:
        # File exists is ok.
        # It may happen if two tasks are racing to create the directory
        if x.errno != errno.EEXIST:
            raise


# About concurrent append operations:
#
# Posix spec says:
#
# If the O_APPEND flag of the file status flags is set, the file
# offset shall be set to the end of the file prior to each write and no
# intervening file modification operation shall occur between changing the
# file offset and the write operation.
#
# See also
# http://www.perlmonks.org/?node_id=486488
#
def safe_append(instream, outfile, timeout = 60):
    def append(outstream):
        while True:
            buf = instream.read(8192)
            if not buf:
                instream.close()
                return
            outstream.write(buf)
    return _safe_fileop(append, "a", outfile, timeout = timeout)

def safe_update(outfile, lines, timeout = 60):
    def update(outstream):
        outstream.seek(0)
        d = dict((x.strip(), True) for x in outstream)
        for x in lines:
            if x not in d:
                outstream.write("%s\n" % x)
    return _safe_fileop(update, "a+", outfile, timeout = timeout)

def _safe_fileop(op, mode, outfile, timeout):
    outstream = file(outfile, mode)
    while timeout > 0:
        try:
            fcntl.flock(outstream, fcntl.LOCK_EX | fcntl.LOCK_NB)
            try:
                r = op(outstream)
                outstream.close()
                return r
            except Exception, x:
                # output file is inconsistent state
                # we must crash the job
                err("Updating file %s failed: %s" %\
                    (outfile, x))
        except IOError, x:
            # Python / BSD doc guides us to check for these errors
            if x.errno in (errno.EACCES, errno.EAGAIN, errno.EWOULDBLOCK):
                time.sleep(0.1)
                timeout -= 0.1
            else:
                raise
    data_err("Timeout when updating file %s" % outfile, outfile)


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
                data_err("Writing external file %s failed"\
                     % fname, fname)
    data_err("Timeout in writing external file %s" % fname, fname)


def write_files(ext_data, path):
    path = os.path.abspath(path)
    ensure_path(path)
    for fname, data in ext_data.iteritems():
        # make sure that no files are written outside the given path
        p = os.path.abspath(os.path.join(path, fname))
        if os.path.dirname(p) == path:
            ensure_file(path + "/" + fname, data = data)
        else:
            err("Unsafe filename %s" % fname)

