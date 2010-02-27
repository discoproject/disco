def nodemux(kvrecord, params):
    yield kvrecord

def iterdemux(k_viter, params):
    k, viter = k_viter
    for v in viter:
        yield k, v

def namedfielddemux(record, params):
    """
    Produce (fieldname, value) pairs for a record.

    Can be used to produce an index of all the possible values of each namedfield.
    """
    return iter(record)

def inverteddemux(record, params):
    """
    Produce ('fieldname:value', record) pairs for a record.

    Can be used to produce an inverted index.
    """
    for item in record:
        yield '%s:%s' % item, record

def invertediddemux(record, params):
    """
    Produce ('fieldname:value', id) pairs for a record.

    Can be used to produce an inverted index when records contain a field named 'id'.
    """
    for item in record:
        yield '%s:%s' % item, record.id
