
:mod:`disco_worker` --- Runtime environment for Disco jobs
==========================================================

.. module:: disco_worker
   :synopsis: Runtime environment for Disco jobs
   
Disco master runs :mod:`disco_worker` to execute map and reduce functions
for a Disco job. The module contains several classes and functions that
are used by Disco internally to connect to input sources, write output
files etc. However, this page only documents features that may be useful
for writing new Disco jobs.

As job functions are imported to the :mod:`disco_worker` namespace
for execution, they can use functions in this module directly without
importing the module explicitely.

.. function:: msg(message)

Sends the string *message* to the master for logging. The message is
shown on the web interface. To prevent a rogue job from overwhelming the
master, the maximum *message* size is set to 255 characters and job is
allowed to send at most 10 messages per second.

.. function:: err(message)

Raises an exception with the reason *message*. This terminates the job.

.. function:: data_err(message)

Raises a data error with the reason *message*. This signals the master to re-run
the task on another node. If the same task raises data error on several
different nodes, the master terminates the job. Thus data error should only be
raised if it is likely that the occurred error is temporary.

Typically this function is used by map readers to signal a temporary failure
in accessing an input file.

.. function:: parse_dir(dir_url)

Parses a directory URL, such as ``dir://nx02/test_simple@12243344`` to
a list of direct URLs. In contrast to other functions in this module,
this function is not used by the job functions, but might be useful for
other programs that need to parse results returned by :func:`disco.job`,
for instance.

.. function:: netstr_reader(fd, size, fname)

A map reader for Disco's internal key-value format. This reader can be
used to read results produced by map and reduce functions. An alias for
:func:`disco.chain_reader`.

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

Note that :func:`disco_worker.re_reader` fails if the input streams contains
unmatched bytes between matched entries. Make sure that your *regexp* is
constructed so that it covers all the bytes in the input stream.

:func:`disco_worker.re_reader` provides an easy way to construct parsers for
textual input streams. For instance, the following reader produces full HTML 
documents as input entries::

        def html_reader(fd, size, fname):
                for x in re_reader("<HTML>(.*?)</HTML>", fd, size, fname):
                        yield x[0]


The default :func:`disco.map_line_reader` is defined as follows::

        def map_line_reader(fd, sze, fname):
                for x in re_reader("(.*?)\n", fd, sze, fname, output_tail = True):
                        yield x[0]

Note that since *output_tail = True* in :func:`disco.map_line_reader`, an input
file that lacks the final newline character is silently accepted.



