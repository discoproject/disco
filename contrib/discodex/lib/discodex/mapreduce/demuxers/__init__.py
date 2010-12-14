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
    recstr = str(record)
    for item in record:
        yield '%s:%s' % item, recstr

def invertediddemux(record, params):
    """
    Produce `('fieldname:value', id)` pairs for a record.

    Can be used to produce an inverted index when records contain a field named `'id'`.
    """
    for item in record:
        yield '%s:%s' % item, record.id

def itemdemux(kvsdict, params):
    """
    Unpacks the kvsdict to produce all `('k:v', kvsdict)` pairs.

    If a key has no values, a `('k', kvsdict)` pair is produced instead.
    """
    import cPickle
    for k, vs in kvsdict.items():
        for key in ('%s:%s' % (k, v) for v in vs) if vs else (k,):
            yield key, cPickle.dumps(kvsdict)
