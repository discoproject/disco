"""
:mod:`disco.bencode` -- Implementation of `Bencode`_
====================================================

.. _Bencode: http://en.wikipedia.org/wiki/Bencode

>>> obj = {'test': {'a': {'deeply': ['nested', {'dict': 45}, 'and', 100000L]}}}
>>> loads(dumps(obj)) == obj
True
"""

def dumps(object):
    import cStringIO
    return dumps2(cStringIO.StringIO(), object).getvalue()

def dumps2(s, object):
    if isinstance(object, (int, long)):
        return encode_int(s, object)
    if isinstance(object, basestring):
        return encode_bytes(s, object)
    if isinstance(object, dict):
        return encode_dict(s, object)
    return encode_list(s, object)

def encode_int(s, object):
    s.write('i%d\n' % object)
    return s

def encode_bytes(s, object):
    s.write('b%d\n' % len(object))
    s.write(object)
    return s

def encode_dict(s, object):
    s.write('d')
    n = len(object)
    for i, (k,v) in enumerate(sorted(object.items())):
        dumps2(s, k).write(',')
        dumps2(s, v)
        if i+1 != n:
            s.write(',')
    s.write('\n')
    return s

def encode_list(s, object):
    s.write('l')
    n = len(object)
    for i, item in enumerate(object):
        dumps2(s, item)
        if i+1 != n:
            s.write(',')
    s.write('\n')
    return s

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
