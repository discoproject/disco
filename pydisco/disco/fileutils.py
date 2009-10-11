
import sys, time, os, fcntl
from disco.util import msg, data_err, err

class AtomicFile(file):
        def __init__(self, fname, *args, **kw):
                ensure_path(fname, False)
                self.fname = fname
                super(AtomicFile, self).__init__(
                        fname + ".partial", *args, **kw)

        def close(self):
                super(AtomicFile, self).close()
                os.rename(self.fname + ".partial", self.fname)
        
class PartitionFile(AtomicFile):
        def __init__(self, partfile, tmpname, *args, **kw):
                self.partfile = partfile
                self.tmpname = tmpname
                super(PartitionFile, self).__init__(
                        tmpname, *args, **kw)

        def close(self):
                super(PartitionFile, self).close()
                safe_append(file(self.tmpname), self.partfile)
                os.remove(self.tmpname)

def ensure_path(path, check_exists = True):
        if check_exists and os.path.exists(path):
                err("File exists: %s" % path)
        if os.path.isfile(path):
                os.remove(path)
        dirpath, fname = os.path.split(path)
        try:
                os.makedirs(dirpath)
        except OSError, x:
                if x.errno == 17:
                        # File exists is ok, it may happen
                        # if two tasks are racing to create
                        # the directory
                        pass
                else:
                        raise x


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
                d = dict((x.strip(), True) for x in outstream)
                outstream.write("\n".join(x for x in lines if x not in d))
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
                        # Python doc guides us to check both the
                        # EWOULDBLOCK (11) and EACCES (13) errors
                        if x.errno == 11 or x.errno == 13:
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
                        if type(data) == str:
                               os.write(fd, data)
                        else:
                               os.write(fd, data())
                        os.close(fd)
                        os.rename(fname + ".partial", fname)
                        return True
                except OSError, x:
                        # File exists
                        if x.errno == 17:
                                time.sleep(1)
                                timeout -= 1
                        else:
                                data_err("Writing external file %s failed"\
                                        % fname, fname)
        data_err("Timeout in writing external file %s" % fname, fname)


def write_files(ext_data, path):
        path = os.path.abspath(path)
        ensure_path(path + "/", False)
        for fname, data in ext_data.iteritems():
                # make sure that no files are written outside the given path
                p = os.path.abspath(os.path.join(path, fname))
                if os.path.dirname(p) == path:
                        ensure_file(path + "/" + fname, data = data)
                else:
                        err("Unsafe filename %s" % fname)

