"""
:mod:`disco.worker.classic.func` --- Functions for constructing Classic Disco jobs
==================================================================================

A Classic Disco job is specified by one or more :term:`job functions`.
This module defines the interfaces for the job functions,
some default values, as well as otherwise useful functions.
"""
import re, cPickle
from disco.error import DataError

def notifier(urls):
    """
    :type urls:  url or list of urls
    :param urls: a list of urls gives replica locators.
    """

def map(entry, params):
    """
    Returns an iterable of (key, value) pairs given an *entry*.

    :param entry: entry coming from the input stream
    :param params: used to maintain state between calls to the map function.

    For instance::

        def fun_map(e, params):
            return [(w, 1) for w in e.split()]

    This example takes a line of text as input in *e*, tokenizes it,
    and returns a list of words as the output.

    The map task can also be an external program.
    For more information, see :ref:`discoext`.
    """

def partition(key, nr_partitions, params):
    """
    Returns an integer in ``range(0, nr_partitions)``.

    :param key: is a key object emitted by a task function
    :param nr_partitions: the number of partitions
    :param params: the object specified by the *params* parameter
    """

def combiner(key, value, buffer, done, params):
    """
    Returns an iterator of ``(key, value)`` pairs or ``None``.

    :param key: key object emitted by the :func:`map`
    :param value: value object emitted by the :func:`map`
    :param buffer: an accumulator object (a dictionary),
                   that combiner can use to save its state.
                   The function must control the *buffer* size,
                   to prevent it from consuming too much memory, by calling
                   ``buffer.clear()`` after each block of results.
    :param done: flag indicating if this is the last call with a given *buffer*
    :param params: the object specified by the *params* parameter

    This function receives all output from the
    :func:`map` before it is saved to intermediate results.
    Only the output produced by this function is saved to the results.

    After :func:`map` has consumed all input entries,
    combiner is called for the last time with the *done* flag set to ``True``.
    This is the last opportunity for the combiner to return something.
    """

def reduce(input_stream, output_stream, params):
    """
    Takes three parameters, and adds reduced output to an output object.

    :param input_stream: :class:`InputStream` object that is used
        to iterate through input entries.
    :param output_stream: :class:`OutputStream` object that is used
        to output results.
    :param params: the object specified by the *params* parameter

    For instance::

        def fun_reduce(iter, out, params):
            d = {}
            for k, v in iter:
                d[k] = d.get(k, 0) + 1
            for k, c in d.iteritems():
                out.add(k, c)

    This example counts how many times each key appears.

    The reduce task can also be an external program.
    For more information, see :ref:`discoext`.
    """

def reduce2(input_stream, params):
    """
    Alternative reduce signature which takes 2 parameters.

    Reduce functions with this signature should return an iterator
    of ``key, value`` pairs, which will be implicitly added to the
    :class:`OutputStream`.

    For instance::

        def fun_reduce(iter, params):
            from disco.util import kvgroup
            for k, vs in kvgroup(sorted(iter)):
                yield k, sum(1 for v in vs)

    This example counts the number of values for each key.
    """

def init(input_iter, params):
    """
    Perform some task initialization.

    :param input_iter: an iterator returned by a :func:`reader`

    Typically this function is used to initialize some modules in the worker
    environment (e.g. ``ctypes.cdll.LoadLibrary()``), to initialize some
    values in *params*, or to skip unneeded entries in the beginning
    of the input stream.
    """

def input_stream(stream, size, url, params):
    """
    :param stream: :class:`InputStream` object
    :param size: size of the input (may be ``None``)
    :param url: url of the input

    Returns a triplet (:class:`InputStream`, size, url) that is
    passed to the next *input_stream* function in the chain. The
    last :class:`disco.func.InputStream` object returned by the chain is used
    to iterate through input entries.

    Using an :func:`input_stream` allows you to customize how input urls are opened.

    Input streams are used for specifying the *map_input_stream*, *map_reader*,
    *reduce_input_stream*, and *reduce_reader* parameters for the
    :class:`disco.worker.classic.worker.Worker`.
    """

def output_stream(stream, partition, url, params):
    """
    :param stream: :class:`OutputStream` object
    :param partition: partition id
    :param url: url of the input

    Returns a pair (:class:`OutputStream`, url) that is
    passed to the next *output_stream* function in the chain. The
    :meth:`OutputStream.add` method of the last
    :class:`OutputStream` object returned by the chain is used
    to output entries from map or reduce.

    Using an :func:`output_stream` allows you to customize where and how
    output is stored. The default should almost always be used.
    """

class OutputStream:
    """
    A file-like object returned by the ``map_output_stream`` or
    ``reduce_output_stream`` chain of :func:`output_stream` functions.
    Used to encode key, value pairs add write them to the underlying file object.
    """
    def add(key, value):
        """
        Adds a key, value pair to the output stream.
        """

    def close(self):
        """
        Close the output stream.
        """

    def write(data):
        """
        .. deprecated:: 0.3

        Writes `data` to the underlying file object.
        """

    @property
    def path(self):
        """
        The path on the local filesystem (used only for saving output to DDFS).
        """

class InputStream:
    """
    A file-like object returned by the ``map_input_stream`` or
    ``reduce_input_stream`` chain of :func:`input_stream` functions.
    Used either to read bytes from the input source or to iterate through input
    entries.
    """
    def __iter__():
        """
        Iterates through input entries. Typically calls *self.read()* to read
        bytes from the underlying file object, which are deserialized to the
        actual input entries.
        """

    def read(num_bytes=None):
        """
        Reads at most *num_bytes* from the input source, or until EOF if *num_bytes*
        is not specified.
        """

def old_netstr_reader(fd, size, fname, head=''):
    """
    Reader for Disco's default/internal key-value format.

    Reads output of a map / reduce job as the input for a new job.
    Specify this function as your :func:`map_reader`
    to use the output of a previous job as input to another job.
    """
    if size is None:
        raise ValueError("Content-length must be defined")

    def read_netstr(idx, data, tot):
        ldata = len(data)
        i = 0
        lenstr = ''
        if ldata - idx < 11:
            data = data[idx:] + fd.read(8192)
            ldata = len(data)
            idx = 0

        i = data.find(' ', idx, idx + 11)
        if i == -1:
            raise DataError("Corrupted input: "\
                            "Could not parse a value length at %d bytes."\
                            % (tot), fname)
        else:
            lenstr = data[idx:i + 1]
            idx = i + 1

        if ldata < i + 1:
            raise DataError("Truncated input: "\
                            "Expected %d bytes, got %d" % (size, tot), fname)

        try:
            llen = int(lenstr)
        except ValueError:
            raise DataError("Corrupted input: "\
                            "Could not parse a value length at %d bytes."\
                            % (tot), fname)

        tot += len(lenstr)

        if ldata - idx < llen + 1:
            data = data[idx:] + fd.read(llen + 8193)
            ldata = len(data)
            idx = 0

        msg = data[idx:idx + llen]

        if idx + llen + 1 > ldata:
            raise DataError("Truncated input: "\
                            "Expected a value of %d bytes (offset %u bytes)"\
                            % (llen + 1, tot), fname)

        tot += llen + 1
        idx += llen + 1
        return idx, data, tot, msg

    data = head + fd.read(8192)
    tot = idx = 0
    while tot < size:
        key = val = ''
        idx, data, tot, key = read_netstr(idx, data, tot)
        idx, data, tot, val = read_netstr(idx, data, tot)
        yield key, val

def re_reader(item_re_str, fd, size, fname, output_tail=False, read_buffer_size=8192):
    """
    A map reader that uses an arbitrary regular expression to parse the input
    stream.

    :param item_re_str: regular expression for matching input items

    The reader works as follows:

     1. X bytes is read from *fd* and appended to an internal buffer *buf*.
     2. ``m = regexp.match(buf)`` is executed.
     3. If *buf* produces a match, ``m.groups()`` is yielded, which contains an
        input entry for the map function. Step 2. is executed for the remaining
        part of *buf*. If no match is made, go to step 1.
     4. If *fd* is exhausted before *size* bytes have been read,
        and *size* tests ``True``,
        a :class:`disco.error.DataError` is raised.
     5. When *fd* is exhausted but *buf* contains unmatched bytes, two modes are
        available: If ``output_tail=True``, the remaining *buf* is yielded as is.
        Otherwise, a message is sent that warns about trailing bytes.
        The remaining *buf* is discarded.

    Note that :func:`re_reader` fails if the input streams contains unmatched
    bytes between matched entries.
    Make sure that your *item_re_str* is constructed so that it covers all
    bytes in the input stream.

    :func:`re_reader` provides an easy way to construct parsers for textual
    input streams.
    For instance, the following reader produces full HTML
    documents as input entries::

        def html_reader(fd, size, fname):
            for x in re_reader("<HTML>(.*?)</HTML>", fd, size, fname):
                yield x[0]

    """
    item_re = re.compile(item_re_str)
    buf = ""
    tot = 0
    while True:
        if size:
            r = fd.read(min(read_buffer_size, size - tot))
        else:
            r = fd.read(read_buffer_size)
        tot += len(r)
        buf += r

        m = item_re.match(buf)
        while m:
            yield m.groups()
            buf = buf[m.end():]
            m = item_re.match(buf)

        if not len(r) or (size!=None and tot >= size):
            if size != None and tot < size:
                raise DataError("Truncated input: "\
                "Expected %d bytes, got %d" % (size, tot), fname)
            if len(buf):
                if output_tail:
                    yield [buf]
                else:
                    print "Couldn't match the last %d bytes in %s. "\
                    "Some bytes may be missing from input." % (len(buf), fname)
            break

def default_partition(key, nr_partitions, params):
    """Returns ``hash(key) % nr_partitions``."""
    return hash(key) % nr_partitions

def make_range_partition(min_val, max_val):
    """
    Returns a new partitioning function that partitions keys in the range
    *[min_val:max_val]* into equal sized partitions.

    The number of partitions is defined by the *partitions* parameter
    """
    r = max_val - min_val
    f = "lambda k, n, p: int(round(float(int(k) - %d) / %d * (n - 1)))" %\
        (min_val, r)
    return eval(f)

def noop(*args, **kwargs):
    pass

def nop_map(entry, params):
    """
    No-op map.

    This function can be used to yield the results from the input stream.
    """
    yield entry

def nop_reduce(iter, out, params):
    """
    No-op reduce.

    This function can be used to combine results per partition from many
    map functions to a single result file per partition.
    """
    for k, v in iter:
        out.add(k, v)

def sum_combiner(key, value, buf, done, params):
    """
    Sums the values for each key.

    This is a convenience function for performing a basic sum in the combiner.
    """
    if not done:
        buf[key] = buf.get(key, 0) + value
    else:
        return buf.iteritems()

def sum_reduce(iter, params):
    """
    Sums the values for each key.

    This is a convenience function for performing a basic sum in the reduce.
    """
    buf = {}
    for key, value in iter:
        buf[key] = buf.get(key, 0) + value
    return buf.iteritems()

def gzip_reader(fd, size, url, params):
    """Wraps the input in a :class:`gzip.GzipFile` object."""
    from gzip import GzipFile
    return GzipFile(fileobj=fd), size, url

def gzip_line_reader(fd, size, url, params):
    """Yields as many lines from the gzipped fd as possible, prints exception if fails."""
    from gzip import GzipFile
    try:
        for line in GzipFile(fileobj=fd):
            yield line
    except Exception, e:
        print e

def task_input_stream(stream, size, url, params):
    """
    An :func:`input_stream` which looks at the scheme of ``url``
    and tries to import a function named ``input_stream``
    from the module ``disco.schemes.scheme_SCHEME``,
    where SCHEME is the parsed scheme.
    If no scheme is found in the url, ``file`` is used.
    The resulting input stream is then used.
    """
    from disco import schemes
    return schemes.input_stream(stream, size, url, params, globals=globals())
map_input_stream = reduce_input_stream = task_input_stream

def string_input_stream(string, size, url, params):
    from cStringIO import StringIO
    return StringIO(string), len(string), url

def task_output_stream(stream, partition, url, params):
    """
    An :func:`output_stream` which returns a handle to a task output.
    The handle ensures that if a task fails, partially written data is ignored.
    """
    from disco.fileutils import AtomicFile
    return AtomicFile(url)
map_output_stream = reduce_output_stream = task_output_stream

def disco_output_stream(stream, partition, url, params):
    """Output stream for Disco's internal compression format."""
    from disco.fileutils import DiscoOutputStream
    return DiscoOutputStream(stream)

def disco_input_stream(stream, size, url, ignore_corrupt = False):
    """Input stream for Disco's internal compression format."""
    import struct, cStringIO, gzip, cPickle, zlib
    offset = 0
    while True:
        header = stream.read(1)
        if not header:
            return
        if ord(header[0]) < 128:
            for e in old_netstr_reader(stream, size, url, header):
                yield e
            return
        try:
            is_compressed, checksum, hunk_size =\
                struct.unpack('<BIQ', stream.read(13))
        except:
            raise DataError("Truncated data at %d bytes" % offset, url)
        if not hunk_size:
            return
        hunk = stream.read(hunk_size)
        data = ''
        try:
            data = zlib.decompress(hunk) if is_compressed else hunk
            if checksum != (zlib.crc32(data) & 0xFFFFFFFF):
                raise ValueError("Checksum does not match")
        except (ValueError, zlib.error), e:
            if not ignore_corrupt:
                raise DataError("Corrupted data between bytes %d-%d: %s" %
                                (offset, offset + hunk_size, e), url)
        offset += hunk_size
        hunk = cStringIO.StringIO(data)
        while True:
            try:
                yield cPickle.load(hunk)
            except EOFError:
                break

class DiscoDBOutput(object):
    def __init__(self, stream, params):
        from discodb import DiscoDBConstructor
        self.discodb_constructor = DiscoDBConstructor()
        self.stream = stream
        self.params = params
        self.path = stream.path

    def add(self, key, val):
        self.discodb_constructor.add(key, val)

    def close(self):
        def flags():
            return dict((flag, getattr(self.params, flag))
                        for flag in ('unique_items', 'disable_compression')
                        if hasattr(self.params, flag))
        self.discodb_constructor.finalize(**flags()).dump(self.stream)

def discodb_output(stream, partition, url, params):
    from disco.worker.classic.func import DiscoDBOutput
    return DiscoDBOutput(stream, params), 'discodb:%s' % url.split(':', 1)[1]

def disk_sort(worker, input, filename, sort_buffer_size='10%'):
    from os.path import getsize
    from disco.comm import open_local
    from disco.util import format_size
    from disco.fileutils import AtomicFile
    worker.send('MSG', "Downloading %s" % filename)
    out_fd = AtomicFile(filename)
    for key, value in input:
        if not isinstance(key, str):
            raise ValueError("Keys must be strings for external sort", key)
        if '\xff' in key or '\x00' in key:
            raise ValueError("Cannot sort key with 0xFF or 0x00 bytes", key)
        else:
            # value pickled using protocol 0 will always be printable ASCII
            out_fd.write('%s\xff%s\x00' % (key, cPickle.dumps(value, 0)))
    out_fd.close()
    worker.send('MSG', "Downloaded %s OK" % format_size(getsize(filename)))
    worker.send('MSG', "Sorting %s..." % filename)
    unix_sort(filename, sort_buffer_size=sort_buffer_size)
    worker.send('MSG', ("Finished sorting"))
    fd = open_local(filename)
    for k, v in re_reader("(?s)(.*?)\xff(.*?)\x00", fd, len(fd), fd.url):
        yield k, cPickle.loads(v)

def unix_sort(filename, sort_buffer_size='10%'):
    import subprocess
    try:
        env = os.environ.copy()
        env['LC_ALL'] = 'C'
        subprocess.check_call(['sort',
                               '-z',
                               '-t', '\xff',
                               '-k', '1,1',
                               '-T', '.',
                               '-S', sort_buffer_size,
                               '-o', filename,
                               filename],
                               env=env)
    except subprocess.CalledProcessError, e:
        raise DataError("Sorting %s failed: %s" % (filename, e), filename)

chain_reader = disco_input_stream
chain_stream = (task_input_stream, chain_reader)
default_stream = (task_input_stream, )
discodb_stream = (task_output_stream, discodb_output)
gzip_stream = (task_input_stream, gzip_reader)
gzip_line_stream = (task_input_stream, gzip_line_reader)
