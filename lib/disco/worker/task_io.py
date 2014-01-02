"""
:mod:`disco.worker.task_io` --- I/O Utility functions for Disco tasks
=====================================================================
"""

from disco import util

def gzip_reader(fd, size, url, params):
    """Wraps the input in a :class:`gzip.GzipFile` object."""
    from gzip import GzipFile
    return GzipFile(fileobj=fd), size, url

def gzip_line_reader(fd, size, url, params):
    """
    Yields as many lines from the gzipped fd as possible, prints
    exception if fails.
    """

    from gzip import GzipFile
    try:
        for line in GzipFile(fileobj=fd):
            yield line
    except Exception as e:
        print(e)

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

def string_input_stream(string, size, url, params):
    from disco.compat import StringIO
    return StringIO(string), len(string), url

def task_output_stream(stream, partition, url, params):
    """
    An :func:`output_stream` which returns a handle to a task output.
    The handle ensures that if a task fails, partially written data is ignored.
    """
    from disco.fileutils import AtomicFile
    return AtomicFile(url)

def disco_output_stream(stream, partition, url, params):
    """Output stream for Disco's internal compression format."""
    from disco.fileutils import DiscoOutputStream
    return DiscoOutputStream(stream)

def disco_input_stream(stream, size, url, ignore_corrupt = False):
    """Input stream for Disco's internal compression format."""
    from disco.compat import BytesIO, int_of_byte
    from disco.compat import pickle_load
    import struct, gzip, zlib
    offset = 0
    while True:
        header = stream.read(1)
        if not header:
            return
        if int_of_byte(header[0]) < 128:
            for e in old_netstr_reader(stream, size, url, header):
                yield e
            return
        try:
            is_compressed, checksum, hunk_size =\
                struct.unpack('<BIQ', stream.read(13))
        except:
            raise DataError("Truncated data at {0} bytes".format(offset), url)
        if not hunk_size:
            return
        hunk = stream.read(hunk_size)
        data = b''
        try:
            data = zlib.decompress(hunk) if is_compressed else hunk
            if checksum != (zlib.crc32(data) & 0xFFFFFFFF):
                raise ValueError("Checksum does not match")
        except (ValueError, zlib.error) as e:
            if not ignore_corrupt:
                raise DataError("Corrupted data between bytes {0}-{1}: {2}"
                                .format(offset, offset + hunk_size, e), url)
        offset += hunk_size
        hunk = BytesIO(data)
        while True:
            try:
                yield pickle_load(hunk)
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
    return DiscoDBOutput(stream, params), 'discodb:{0}'.format(url.split(':', 1)[1])

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

def old_netstr_reader(fd, size, fname, head=b''):
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
            data = data[idx:] + bytes_to_str(fd.read(8192))
            ldata = len(data)
            idx = 0

        i = data.find(' ', idx, idx + 11)
        if i == -1:
            raise DataError("Corrupted input: "
                            "Could not parse a value length at {0} bytes."
                            .format(tot), fname)
        else:
            lenstr = data[idx:i + 1]
            idx = i + 1

        if ldata < i + 1:
            raise DataError("Truncated input: "
                            "Expected {0} bytes, got {1}"
                            .format(size, tot), fname)

        try:
            llen = int(lenstr)
        except ValueError:
            raise DataError("Corrupted input: "
                            "Could not parse a value length at {0} bytes."
                            .format(tot), fname)

        tot += len(lenstr)

        if ldata - idx < llen + 1:
            data = data[idx:] + bytes_to_str(fd.read(llen + 8193))
            ldata = len(data)
            idx = 0

        msg = data[idx:idx + llen]

        if idx + llen + 1 > ldata:
            raise DataError("Truncated input: "
                            "Expected a value of {0} bytes (offset {1} bytes)"
                            .format(llen + 1, tot), fname)

        tot += llen + 1
        idx += llen + 1
        return idx, data, tot, msg

    data = bytes_to_str(head + fd.read(8192))
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
    import re
    item_re = re.compile(item_re_str)
    buf = b""
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
                raise DataError("Truncated input: "
                                "Expected {0} bytes, got {1}"
                                .format(size, tot), fname)
            if len(buf):
                if output_tail:
                    yield [buf]
                else:
                    print("Couldn't match the last {0} bytes in {1}. "
                          "Some bytes may be missing from input.".format(len(buf), fname))
            break

chain_reader = disco_input_stream
chain_stream = (task_input_stream, chain_reader)
default_stream = (task_input_stream, )
discodb_stream = (task_output_stream, discodb_output)
gzip_stream = (task_input_stream, gzip_reader)
gzip_line_stream = (task_input_stream, gzip_line_reader)

class ClassicFile(object):
    def __init__(self, url, streams, params, fd=None, size=None):
        self.fds = []
        for stream in streams:
            maybe_params = (params,) if util.argcount(stream) == 4 else ()
            fd = stream(fd, size, url, *maybe_params)
            if isinstance(fd, tuple):
                if len(fd) == 3:
                    fd, size, url = fd
                else:
                    fd, url = fd
            self.fds.append(fd)

    def __iter__(self):
        return iter(self.fds[-1])

    def close(self):
        for fd in reversed(self.fds):
            if hasattr(fd, 'close'):
                fd.close()
