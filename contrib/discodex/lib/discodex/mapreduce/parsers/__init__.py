"""
:mod:`discodex.mapreduce.parsers` -- builtin parsers
====================================================

Parsers are essentially the :func:`map_reader <disco.func.reader>` function
for the :class:`discodex.mapreduce.Indexer`.

A parser takes a chunk of a dataset and produces zero or more records
(see :mod:`discodex.mapreduce.demuxers`).
"""

def noparse(iterable, size, fname, params):
    """Returns the iterable."""
    return iterable

def rawparse(iterable, size, fname, params):
    """
    Maps `raw` URLs to (key, value) pairs.

    e.g. `raw://a:b,c:d,e:f` yields `[(a, b), (c, d), (e, f)]`
    """
    for line in iterable:
        for item in line.strip('/').split('/'):
            for kv in item.split(','):
                yield kv.split(':', 1)

def wordparse(iterable, size, fname, params):
    """Splits lines of input by whitespace and uses the words as keys for the value ``fname``"""
    for word in set(word for line in iterable for word in line.split()):
        yield word, fname

def netstrparse(fd, size, fname, params):
    """Reads (key, value) pairs directly from `netstr` input."""
    from disco import func
    return func.netstr_reader(fd, size, fname, params)

def discodbparse(iterable, size, fname, params):
    """Splits lines of input by whitespace and uses the fields as keys for a :class:`discodb.DiscoDB` objects."""
    from discodb import DiscoDB
    for line in iterable:
        yield DiscoDB((field, []) for field in line.split())

def recordparse(iterable, size, fname, params):
    """Splits lines of input by whitespace and creates :class:`discodex.mapreduce.Record` objects."""
    from discodex.mapreduce import Record
    for line in iterable:
        yield Record(*line.split())

def csvrecordparse(iterable, size, fname, params):
    """Splits lines of input by commas and creates :class:`discodex.mapreduce.Record` objects."""
    from discodex.mapreduce import Record
    for line in iterable:
        yield Record(*line.strip().split(','))

def enumfieldparse(iterable, size, fname, params):
    """Like :func:`recordparse` except fields are named by the column they appear in."""
    from discodex.mapreduce import Record
    for line in iterable:
        yield Record(**dict((str(n), f) for n, f in enumerate(line.split())))
