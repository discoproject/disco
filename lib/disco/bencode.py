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

def loads(string):
    object, rest = decode(string)
    return object

def decode_int(string):
    end = string.index('e')
    return int(string[:end]), string[end + 1:]

def decode_bytes(string):
    length, string = string.split(':', 1)
    return string[:int(length)], string[int(length):]

def decode_dict(string):
    list, rest = decode_list(string)
    return dict(n_at_a_time(list, 2)), rest

def decode_list(string):
    objects = []
    while not string.startswith('e'):
        object, string = decode(string)
        objects.append(object)
    return objects, string[1:]

def decode(string):
    if string.startswith('i'):
        return decode_int(string[1:])
    if string.startswith('d'):
        return decode_dict(string[1:])
    if string.startswith('l'):
        return decode_list(string[1:])
    return decode_bytes(string)

def encode_int(object):
    return 'i%de' % object

def encode_bytes(object):
    return '%d:%s' % (len(object), object)

def encode_dict(object):
    return 'd%se' % ''.join('%s%s' % (dumps(k), dumps(v))
                            for k, v in sorted(object.items()))

def encode_list(object):
    return 'l%se' % ''.join(dumps(item) for item in object)

def n_at_a_time(iter, n):
    from itertools import cycle, groupby
    def key(object, pattern=cycle(x // n for x in xrange(2 * n))):
        return pattern.next()
    return (tuple(vs) for k, vs in groupby(iter, key=key))
