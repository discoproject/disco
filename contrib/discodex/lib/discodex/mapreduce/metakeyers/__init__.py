"""
:mod:`discodex.mapreduce.metakeyers` -- builtin metakeyers
==========================================================

Metakeyers are essentially the map function for the :class:`discodex.mapreduce.MetaIndexer`.

The metakeyer is called for every `key` in the index and produces zero or more `(metakey, value)` pairs.
"""

def prefixkeyer(key, params):
    """Produces `(prefix, key)` pairs for every possible prefix in the key."""
    for n, letter in enumerate(key):
        yield key[:n + 1], key
