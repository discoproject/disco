"""
Tools to help in constructing DiscoDB objects.

>>> kvlist = [('k', 'v'), ('k', 'v2'), ('k2', 'v2')]
>>> [(k, list(v)) for k, v in kvgroup(kvlist)]
[('k', ['v', 'v2']), ('k2', ['v2'])]
"""

from itertools import groupby

def key(kv):
    k, v = kv
    return k

def kvgroup(kviter):
    for k, kvs in groupby(kviter, key):
        yield k, (v for _k, v in kvs)
