"""
.. _discoext:

:mod:`disco.node.external` - Disco External Interface
=====================================================

Disco provides an external interface for specifying map and reduce
functions as external programs, instead of Python functions. This feature
is useful if you have already an existing program or a library which could be
useful for a Disco job, or your map / reduce :term:`task` is severely CPU
or memory-bound and implementing it, say, in C, would remedy the problem.

Note that currently the external interface is not suitable for speeding up
jobs that are mostly IO bound, or slowed down due to the overhead caused
by Disco. Actually, since the external interface uses the standard input
and output for communicating with the process, the overhead caused by
Disco is likely to increase when using the external interface. However,
if the task is CPU or memory-bound, the additional communication overhead
is probably minimal compared to gained benefits.

Easy approach using the `ctypes` module
---------------------------------------

In many cases there is an easier alternative to the external interface:
You can write the CPU-intensive functions in C and compile them to
a shared library which can be included in the *required_files* list
of :meth:`disco.core.Disco.new_job`. Here is an example::

        def fast_map(e, params):
                return [("", params.mylib.fast_function(e))]

        def map_init(iter, params):
                ctypes.cdll.LoadLibrary("mylib.so")
                params.mylib = ctypes.CDLL("mylib.so")

        Disco("disco://discomaster").new_job(
                name = "mylib_job",
                input = ["http://someinput"],
                map = fast_map,
                map_init = map_init,
                required_files = ["mylib.so"],
                required_modules = ["ctypes"])

If this approach works for you, there is no need to read this document further.
For more information, see documentation of the `ctypes module
<http://docs.python.org/library/ctypes.html>`_.

External interface
------------------

The external program reads key-value pairs from the standard input and outputs
key-value pairs to the standard output. In addition, the program may read
parameters from the standard input when the task starts, and it may output log
messages to the standard error stream. This interface should be easy to
implement in any programming language, although C is used in examples below.

The key-value pairs are both read and written in the following format::

        <key-size><key><value-size><value>

Here *key-size* and *value-size* are 32-bit integers, encoded in
little-endian, which specify the sizes of the key and the value in bytes. *key*
and *value* correspond to the key and the value strings.

For instance, the following C function reads a key-value pair from the standard
input:

.. code-block:: c

        void read_kv(char **key, char **val)
        {
                unsigned int len;
                *key = *val = NULL;
                /* read key */
                if (!fread(&len, 4, 1, stdin))
                        return 0;
                if (len){
                        *key = malloc(len);
                        fread(*key, len, 1, stdin);
                }
                /* read value */
                fread(&len, 4, 1, stdin);
                if (len){
                        *val = malloc(len);
                        fread(*val, len, 1, stdin);
                }
                return 1;
        }

Outputting a key-value pair works correspondingly using *fwrite()*. Using the
function defined above, one can iterate through all input pairs as follows:

.. code-block:: c

        char *key, *val;
        while (read_kv(&key, &val)){
                /* do something with key and value */
                free(key);
                free(val);
        }

The external program must read key-value pairs from the standard input
as long as there is data available. The program must not exit before
all the input is consumed.

Note that extra care must be taken with buffering of the standard output, so
that the output pairs are actually sent to the receiving program, and not kept
in an internal buffer. Call *fflush(stdout)* if unsure.

Map and reduce tasks follow slightly different interfaces, as specified below.

External map
''''''''''''

An external map task must read a key-value pair from *stdin* as specified above,
and before reading the next pair, output a result list which may be empty. The
output list is defined as follows::

        <num-pairs>[<pair_0>...<pair_{num_pairs}>]

where *num-pairs* is a 32-bit integer, which may be zero. It is followed by
exactly *num-pairs* consequent key-value pairs as defined above.

Inputs for the external map are read using the provided *map_reader*. The
map reader may produce each input entry as a single string (like the
default :func:`disco.func.map_line_reader` does) that is used as the value
in a key-value pair where the key is an empty string. Alternatively,
the reader may return a pair of strings as a tuple, in which case both
the key and the value are specified.

The map finishes when the result list for the final key-value pair
is received.

External reduce
'''''''''''''''

In contrast to the external map, the external reduce is not required
to match each input with a result list. Instead, the external reduce
may output a result list, as specified above, any time it wants, also
after all the inputs have been exhausted. As an extreme case, it may
not produce any output at all.

The reduce finishes when the program exits.

Logging
'''''''

When outputting messages to the standard error, the following format must be
used

.. code-block:: c

   void msg(const char *msg){
        fprintf(stderr, "**<MSG> %s\\n", msg);
   }

   void die(const char *msg){
        fprintf(stderr, "**<ERR> %s\\n", msg);
        exit(1);
   }

Each line must have the first seven bytes as defined above, and the
line must end with a newline character. The *msg()* function above is
subject to the same limits as the standard :func:`disco_worker.msg`
message function.

Parameters
''''''''''

Any parameters for the external program must be specified in the
*ext_params* parameter for :func:`disco.core.Job`. If *ext_params* is specified
as a string, Disco will provide it as is for the external program in the
standard input, before any key-value pairs. It is on the responsibility
of the external program to read all bytes that belong to the parameter set
before starting to receive key-value pairs.

As a special case, the standard C interface for Disco, as specified
below, accepts a dictionary of string-string pairs as *ext_params*. The
dictionary is then encoded by :func:`disco.core.Job` using the *netstring*
module. The *netstring* format is extremely simple, consisting of consequent
key-value pairs. An example how to parse parameters in this case can be
found in the :cfunc:`read_parameters` function in *ext/disco.c*.

Usage
-----

An external task consists of a single executable main program and an
arbitrary number of supporting files. All the files are written to a
single flat directory on the target node, so the program must be prepared
to access any supporting files on its current working directory, including
any libraries it needs.

Any special settings, or environment variables, that the program needs to
be set can be usually arranged by a separate shell script that prepares
the environment before running the actual executable. In that case your
main program will be the shell script, and the actual executable one of
the supporting files.

An external program absolutely must not read any files besides the ones
included in its supporting files. It must not write to any files on its
host, to ensure integrity of the runtime environment.

An external map or reduce task is specified by giving a dictionary, instead of a
function, as the *fun_map* or *reduce* parameter in :func:`disco.core.Job`. The
dictionary contains at least a single key-value pair where key is the string
*"op"* and the value the actual executable code. Here's an example::

        disco.job("disco://localhost:5000",
                  ["disco://localhost/myjob/file1"],
                  fun_map = {"op": file("bin/external_map").read(),
                             "config.txt": file("bin/config.txt").read()})

The dictionary may contain other keys as well, which correspond to the
file names (not paths) of the supporting files, such as *"config.txt"*
above. The corresponding values must contain the contents of the
supporting files as strings.

A convenience function :func:`disco.util.external` is provided for constructing the
dictionary that specifies an external task. Here's the same example as above but
using :func:`disco.util.external`::

        disco.job("disco://localhost:5000",
                  ["disco://localhost/myjob/file1"],
                  fun_map = disco.external(["bin/external_map", "bin/config.txt"]))

Note that the first file in the list must be the actual executable. The rest of
the paths may point at the supporting files in an arbitrary order.

Disco C library
---------------

Disco comes with a tiny C file, *ext/disco.c* and a header, *ext/disco.h*
which wrap the external interface behind a few simple functions. The
library takes care of allocating memory for incoming key-value pairs,
without doing malloc-free for each pair separately. It also takes care
of reading a parameter dictionary to a `Judy array <http://judy.sf.net>`_
which is like a dictionary object for C.

Here's a simple external map program that echoes back each key-value pair,
illustriating usage of the library.

.. code-block:: c

        #include <disco.h>

        int main(int argc, char **argv)
        {
                const Pvoid_t params = read_parameters();
                Word_t *ptr;
                JSLG(ptr, params, "some parameter");
                if (!ptr)
                        die("parameter missing");

                p_entry *key = NULL;
                p_entry *val = NULL;

                int i = 0;
                while (read_kv(&key, &val)){
                        if (!(i++ % 10000))
                                msg("Got key <%s> val <%s>", key->data, val->data);
                        write_num_prefix(1);
                        write_kv(key, val);
                }
                msg("%d key-value pairs read ok", i);
                return 0;
        }

The following functions are available in the library

.. cfunction:: Pvoid_t read_parameters()

   This function must be called before any call to the function
   :cfunc:`read_kv`. It returns the parameter dictionary
   as a Judy array of type *JudySL*. See `JudySL man page <http://judy.sourceforge.net/doc/JudySL_3x.htm>`_ for more information.

.. cfunction:: void die(const char *msg)

   .. **

   Kills the job with the message *msg*.

.. cfunction:: int read_kv(p_entry **key, p_entry **val)

   .. ***

   Reads a key-value pair from the standard input. :cfunc:`read_kv`
   can re-use *key* and *value* across many calls, so there is no need
   to *free()* them explicitely. If you need to save a key-value pair
   on some iteration, use :cfunc:`copy_entry` to make a copy of the
   desired entry. Naturally you are responsible for freeing any copy that
   isn't needed anymore, unless you re-use it as a :cfunc:`copy_entry`
   destination. To summarize, you need to call *free()* for entries that
   won't be re-used in a :cfunc:`copy_entry` or :cfunc:`read_kv` call.

   Returns key and value strings in :ctype:`p_entry` structs.

        .. ctype:: p_entry

           Container type for a string.

        .. cmember:: p_entry.len

           Length of the string

        .. cmember:: p_entry.sze

           Size of the allocated buffer. Always holds *len <= sze*.

        .. cmember:: p_entry.data

           Actual string of the size *len*, ending with an additional zero byte.

.. cfunction:: void write_num_prefix(int num)

   Writes the *num_pairs* prefix for the result list as defined above. This call
   must be followed by *num* :cfunc:`write_kv` calls.

.. cfunction:: void write_kv(const p_entry *key, const p_entry *val)

   .. **

   Writes a key-value pair to the standard output. Must be preceded with a
   :cfunc:`write_num_prefix` call.

In addition, the library contains the following utility functions:

.. cfunction:: void *dxmalloc(unsigned int size)

   .. **

   Tries to allocate *size* bytes. Exits with :cfunc:`die` if allocation fails.

.. cfunction:: void copy_entry(p_entry **dst, const p_entry *src)

   .. ***

   Copies *src* to *dst*. Grows *dst* if needed, or allocates a new
   :ctype:`p_entry` if *dst = NULL*.
"""
import os
import os.path
import time
import struct
import marshal

from subprocess import Popen, PIPE
from disco.netstring import decode_netstring_str, encode_netstring_fd
from disco.fileutils import write_files
from disco.util import msg
from disco.error import DiscoError

MAX_ITEM_SIZE = 1024**3
MAX_NUM_OUTPUT = 1000000

proc, in_fd, out_fd = None, None, None

def pack_kv(k, v):
    return struct.pack("I", len(k)) + k +\
           struct.pack("I", len(v)) + v

def unpack_kv():
    le = struct.unpack("I", out_fd.read(4))[0]
    if le > MAX_ITEM_SIZE:
        raise DiscoError("External key size exceeded: %d bytes" % le)
    k = out_fd.read(le)
    le = struct.unpack("I", out_fd.read(4))[0]
    if le > MAX_ITEM_SIZE:
        raise DiscoError("External key size exceeded: %d bytes" % le)
    v = out_fd.read(le)
    return k, v

def ext_map(e, params):
    if isinstance(e, basestring):
        k = ""
        v = e
    else:
        k, v = e
    in_fd.write(pack_kv(k, v))
    in_fd.flush()
    num = struct.unpack("I", out_fd.read(4))[0]
    r = [unpack_kv() for i in range(num)]
    return r

def ext_reduce(red_in, red_out, params):
    import select
    p = select.poll()
    eof = select.POLLHUP | select.POLLNVAL | select.POLLERR
    p.register(out_fd, select.POLLIN | eof)
    p.register(in_fd, select.POLLOUT | eof)
    MAX_NUM_OUTPUT = MAX_NUM_OUTPUT

    tt = 0
    while True:
        for fd, event in p.poll():
            if event & (select.POLLNVAL | select.POLLERR):
                raise DiscoError("Pipe to the external process failed")
            elif event & select.POLLIN:
                num = struct.unpack("I",
                    out_fd.read(4))[0]
                if num > MAX_NUM_OUTPUT:
                    raise DiscoError("External output limit "\
                        "exceeded: %d > %d" %\
                        (num, MAX_NUM_OUTPUT))
                for i in range(num):
                    red_out.add(*unpack_kv())
                    tt += 1
            elif event & select.POLLOUT:
                try:
                    msg = pack_kv(*red_in.next())
                    in_fd.write(msg)
                    in_fd.flush()
                except StopIteration:
                    p.unregister(in_fd)
                    in_fd.close()
            else:
                return

def prepare(ext_task, params, path):
    params = encode_netstring_fd(params)\
        if params and isinstance(params, dict) else "0\n"
    write_files(ext_task, path)
    open_ext(path + "/op", params)

def open_ext(fname, params):
    # XXX! Run external programs in /data/ dir, not /temp/
    global proc, in_fd, out_fd
    proc = Popen([fname], stdin=PIPE, stdout=PIPE)
    in_fd = proc.stdin
    out_fd = proc.stdout
    in_fd.write(params)

def close_ext():
    if proc:
        os.kill(proc.pid, 9)

