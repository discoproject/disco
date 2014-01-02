"""
:mod:`disco.worker.classic.func` --- Functions for constructing Classic Disco jobs
==================================================================================

A Classic Disco job is specified by one or more :term:`job functions`.
This module defines the interfaces for the job functions,
some default values, as well as otherwise useful functions.
"""
import re
from disco.compat import pickle_loads, pickle_dumps, bytes_to_str, str_to_bytes, sort_cmd
from disco.error import DataError
from disco.worker.task_io import *

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
                   ``buffer.clear()`` after each block of results. Note that
                   each partition (as determined by the key and
                   :func:`partition`) gets its own buffer object.
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
    f = ("lambda k_n_p: int(round(float(int(k_n_p[0]) - {0}) / {1} * (k_n_p[1] - 1)))"
         .format(min_val, r))
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
        return buf.items()

def sum_reduce(iter, params):
    """
    Sums the values for each key.

    This is a convenience function for performing a basic sum in the reduce.
    """
    buf = {}
    for key, value in iter:
        buf[key] = buf.get(key, 0) + value
    return buf.items()

map_input_stream = reduce_input_stream = task_input_stream

map_output_stream = reduce_output_stream = task_output_stream
