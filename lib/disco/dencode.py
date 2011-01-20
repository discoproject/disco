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

def decode_int(file):
    return int(file.readline()[:-1])

def decode_bytes(file):
    length = decode_int(file)
    return file.read(length)

def decode_dict(file):
    return dict(n_at_a_time(decode_list(file), 2))

def decode_list(file):
    return list(decode_iter(file))

def decode_iter(file):
    separator = ','
    while separator == ',':
        yield decode(file)
        separator = file.read(1)

def decode(file):
    return {'i': decode_int,
            'd': decode_dict,
            'l': decode_list,
            'b': decode_bytes}[file.read(1)](file)

def n_at_a_time(iter, n):
    from itertools import cycle, groupby
    def key(object, pattern=cycle(x // n for x in xrange(2 * n))):
        return pattern.next()
    return (tuple(vs) for k, vs in groupby(iter, key=key))
