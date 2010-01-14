
:mod:`disco.func` --- Functions for constructing Disco jobs
===========================================================

.. module:: disco.func
   :synopsis: Functions for constructing Disco jobs

A Disco job is specified by one or more user-defined :term:`job
functions`, namely *map*, *reduce*, *combiner* and *partitioner* functions
(see :class:`disco.core.Job` for more information). Of these functions,
only *map* is required. If a job function is not specified by the user,
the corresponding default function that is specified in this module,
is used instead.

.. hint::
   When writing custom functions, take into account the following
   features of the disco worker environment:

            - Only the specified function is included in the request. The function
              can't call any other functions specified in your source file nor can it
              refer to any global names, including any imported modules. If you need
              a special module in your function, import it within the function body.
              Use of global variables or functions with side-effects, such as
              external files besides the given input, is strongly discouraged.

            - The function should not print anything to stderr.
              The task uses stderr to signal events to the master.
              You can raise a :class:`disco.error.DataError`, to abort the task on this node and
              request transfer to another node. Otherwise it is a good idea to just
              let the task die if any exceptions occur -- do not catch any exceptions
              from which you can't recover.
              When other types of exceptions occur, the disco worker will catch them and
              signal an appropriate event to the master.

              In short, this means all user-provded functions must be pure (see
              :term:`pure function`).

.. function:: netstr_reader(fd, size, fname)

   Reader for Disco's internal key-value format. This reader can be
   used to read results produced by map and reduce functions.

.. function:: netstr_writer(fd, key, value, params)

   Writer for Disco's internal key-value format. This reader is used
   to write results in map and reduce functions.

.. function:: object_reader(fd, sze, fname)

   A wrapper for :func:`netstr_reader` that uses Python's ``cPickle``
   module to serialize arbitrary Python objects to strings.

.. function:: object_writer(fd, key, value, params)

   A wrapper for :func:`netstr_writer` that uses Python's ``cPickle``
   module to deserialize strings to Python objects.

.. function:: chain_reader(fd, sze, fname)

   Reads output of a map / reduce job as the input for a new job. You
   must specify this function as *map_reader* for :class:`disco.core.Job`
   if you want to use outputs of a previous map / reduce job as the input
   for another job. An alias for :func:`netstr_reader`.

.. function:: re_reader(regexp, fd, size, fname[, output_tail])

    A map reader that uses an arbitrary regular expression to parse the input
    stream. The desired regular expression is specified in *regexp*. The reader
    works as follows:

     1. X bytes is read from *fd* and appended to an internal buffer *buf*.
     2. ``m = regexp.match(buf)`` is executed.
     3. If *buf* produces a match, ``m.groups()`` is yielded, which contains an
        input entry for the map function. Step 2. is executed for the remaining
        part of *buf*. If no match is made, go to step 1.
     4. If *fd* is exhausted before *size* bytes have been read, a data error is
        raised, unless *size* is not specified.
     5. When *fd* is exhausted but *buf* contains unmatched bytes, two modes are
        available: If *output_tail = True*, the remaining *buf* is yielded as is.
        Otherwise, which is the default case, a message is sent that warns about
        trailing bytes and the remaining *buf* is discarded.

    Note that :func:`re_reader` fails if the input streams contains unmatched
    bytes between matched entries. Make sure that your *regexp* is constructed
    so that it covers all the bytes in the input stream.

    :func:`re_reader` provides an easy way to construct parsers for textual
    input streams. For instance, the following reader produces full HTML
    documents as input entries::

            def html_reader(fd, size, fname):
                    for x in re_reader("<HTML>(.*?)</HTML>", fd, size, fname):
                            yield x[0]


    Another example is the default :func:`map_line_reader`, which is defined as follows::

            def map_line_reader(fd, sze, fname):
                    for x in re_reader("(.*?)\n", fd, sze, fname, output_tail = True):
                            yield x[0]

    Note that since *output_tail = True* in :func:`map_line_reader`, an input
    file that lacks the final newline character is silently accepted.

.. function:: map_input_stream(stream, size, url, params)

   An input stream which looks at the scheme of ``url`` and tries to import a function named ``input_stream`` from the module ``disco.schemes.scheme_SCHEME``, where SCHEME is the parsed scheme.
   If no scheme is found in the url, ``file`` is used.
   The resulting input stream is then used.

.. function:: reduce_input_stream(stream, size, url, params)

   Same as :func:`map_input_stream`.

.. function:: map_output_stream(stream, partition, url, params)

   An output stream which returns a file handle to an appropriate partition output file.
   The file handle ensures that if the task fails prematurely, partial data is not seen.

.. function:: reduce_output_stream(stream, partition, url, params)

   An output stream which returns a file handle to an appropriate reduce output file.
   The file handle ensures that if the task fails prematurely, partial data is not seen.

.. function:: default_partition(key, nr_reduces, params)

   Default partitioning function. Defined as::

        def default_partition(key, nr_reduces, params):
                return hash(str(key)) % nr_reduces

.. function:: make_range_partition(min_val, max_val)

   Returns a new partitioning function that partitions keys in the range
   *[min_val:max_val]* to equal sized partitions. The number of partitions is
   defined by *nr_reduces* in :class:`disco.core.Job`.

.. function:: nop_reduce(iter, out, params)

   No-op reduce. Defined as::

        for k, v in iter:
                out.add(k, v)

   This function can be used to combine results per partition from many
   map functions to a single result file per partition.

.. function:: map_line_reader(fd, sze, fname)

   Default input reader function. Reads inputs line by line.


