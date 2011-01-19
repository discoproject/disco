"""
:mod:`disco.bencode` -- Implementation of `Bencode`_
====================================================

.. _Bencode: http://en.wikipedia.org/wiki/Bencode

>>> obj = {'test': {'a': {'deeply': ['nested', {'dict': 45}, 'and', 100000L]}}}
>>> loads(dumps(obj)) == obj
True
"""
def dumps(object):
    if isinstance(object, (int, long)):
        return encode_int(object)
    if isinstance(object, basestring):
        return encode_bytes(object)
    if isinstance(object, dict):
        return encode_dict(object)
    return encode_list(object)

def encode_int(object):
    return 'i%d\n' % object

def encode_bytes(object):
    return 'b%d\n%s' % (len(object), object)

def encode_dict(object):
    return 'd%s\n' % ','.join('%s,%s' % (dumps(k), dumps(v))
                             for k, v in sorted(object.items()))

def encode_list(object):
    return 'l%s\n' % ','.join(dumps(item) for item in object)

def loads(string):
    from cStringIO import StringIO
    return decode(StringIO(string))

def load(handle):
    return decode(handle)

def decode_int(buf):
    return int(buf.readline()[:-1])

def decode_bytes(buf):
    length = decode_int(buf)
    return buf.read(length)

def decode_dict(buf):
    return dict(n_at_a_time(decode_list(buf), 2))

def decode_list(buf):
    return list(decode_iter(buf))

def decode_iter(buf):
    separator = ','
    while separator == ',':
        yield decode(buf)
        separator = buf.read(1)

def decode(buf):
    return {'i': decode_int,
            'd': decode_dict,
            'l': decode_list,
            'b': decode_bytes}[buf.read(1)](buf)

def n_at_a_time(iter, n):
    from itertools import cycle, groupby
    def key(object, pattern=cycle(x // n for x in xrange(2 * n))):
        return pattern.next()
    return (tuple(vs) for k, vs in groupby(iter, key=key))
