"""
Tools to help in constructing DiscoDB objects.

>>> kvlist = [('k', 'v'), ('k', 'v2'), ('k2', 'v2')]
>>> [(k, list(v)) for k, v in kvgroup(kvlist)]
[('k', ['v', 'v2']), ('k2', ['v2'])]
>>> kvslist = [('k', 'v'), ('k2', ['vs']), ('k', 'v2')]
>>> [(k, list(vs)) for k, vs in normalize(kvslist)]
[('k', ['v', 'v2']), ('k2', ['vs'])]
"""

from itertools import chain, groupby

def iterify(object):
    if hasattr(object, '__iter__'):
        return object
    return object,

def key(kv):
    k, v = kv
    return k

def kvgroup(kviter):
    """Like itertools.groupby, but iterates over (k, vs) instead of (k, k-vs).

    The result can be used to construct a :class:`discodb.DiscoDB`,
    iff ``kviter`` is in sorted order.
    """
    for k, kvs in groupby(kviter, key):
        yield k, (v for _k, v in kvs)

def normalize(iter):
    for k, vss in kvgroup(sorted(iter)):
        yield k, chain(*(iterify(vs) for vs in vss))
