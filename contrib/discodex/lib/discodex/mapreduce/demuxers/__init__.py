"""
:mod:`discodex.mapreduce.demuxers` -- builtin demuxers
======================================================

Demuxers are essentially the :func:`map <disco.func.map>` function for the
:class:`discodex.mapreduce.Indexer`.

A demuxer takes a record (see :mod:`discodex.mapreduce.parsers`) and produces
zero or more `(key, value)` pairs to be stored in  the index.
"""

def nodemux(kvrecord, params):
    """Yields the `kvrecord` itself."""
    yield kvrecord

def iterdemux((k, viter), params):
    """Ungroups the `(k, viter)` to produce `(k, v)` for every `v` in `viter`."""
    for v in viter:
        yield k, v

def namedfielddemux(record, params):
    """
    Produce `(fieldname, value)` pairs for a record.

    Can be used to produce an index of all the possible values of each namedfield.
    """
    return iter(record)

def inverteddemux(record, params):
    """
    Produce `('fieldname:value', record)` pairs for a record.

    Can be used to produce an inverted index.
    """
    for item in record:
        yield '%s:%s' % item, record

def invertediddemux(record, params):
    """
    Produce `('fieldname:value', id)` pairs for a record.

    Can be used to produce an inverted index when records contain a field named `'id'`.
    """
    for item in record:
        yield '%s:%s' % item, record.id
