"""
:mod:`discodex.mapreduce.balancers` -- builtin balancers
========================================================

Balancers are essentially the partition function for the :class:`discodex.mapreduce.Indexer`.

The balancer is called for every `(key, value)` pair (see :mod:`discodex.mapreduce.demuxers`) and returns an integer indicating which partition it belongs in.
"""

def nchunksbalance(key, nr_reduces, params):
    """Randomly chooses a partition."""
    from random import randint
    return randint(0, nr_reduces - 1)

def roundrobinbalance(key, nr_reduces, params):
    """Cycles through the partitions in a round robin fashion."""
    params.n += 1
    return params.n % nr_reduces
