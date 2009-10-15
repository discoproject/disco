from .. import module

demuxer = module(__name__)

@demuxer
def nodemux(kvrecord, params):
    yield kvrecord

@demuxer
def iterdemux(k_viter, params):
    k, viter = k_viter
    for v in viter:
        yield k, v

@demuxer
def namedfielddemux(record, params):
    """
    Produce (fieldname, value) pairs for a record.

    Can be used to produce an index of all the possible values of each namedfield.
    """
    return iter(record)

@demuxer
def inverteddemux(record, params):
    """
    Produce ('fieldname:value', record) pairs for a record.

    Can be used to produce an inverted index.
    """
    for item in record:
        yield '%s:%s' % item, record

@demuxer
def invertediddemux(record, params):
    """
    Produce ('fieldname:value', id) pairs for a record.

    Can be used to produce an inverted index when records contain a field named 'id'.
    """
    for item in record:
        yield '%s:%s' % item, record.id
