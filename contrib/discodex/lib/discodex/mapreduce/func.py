"""
This module contains utility functions used by :mod:`discodex.mapreduce` to construct filter chains plus some builtin filters.

A filter chain is just a sequence of single-argument functions.
Since filter chains are usually specified as a serialized string (i.e. within a URL),
we provide support here for serializing/deserializing the chain.

A single serialized filter is either a fully qualified name, or a name in this modules namespace.
In addition, the filter string can provide an optional string argument,
which will be curried to the function to create a filter.
When a function is specified with optional string argument, the argument is completely url-unquoted.
This is done to support the ReST API, which requires '|' and '}' characters to be url-quoted in filter arguments.

In order to construct filter chains, filter functions must have a particular signature.
A filter receives a single argument, which depends on the context in which the filter is applied, and returns a sequence.
The most general filter has the following signature:
f(x) -> v, ...

Any filter at the end of a chain must return a sequence of (k, v) pairs:
f(x) -> (k, v), ...

In other contexts, other assumptions may also be made.
For instance, the first filter in a reduce chain will always have the following signature:
f((k, vs)) -> v, ...

The first filter in a map chain from a `DiscoDB` `items` resource (e.g. `/indices/[index]/items`) is also guaranteed to have the same signature.
The first filter in a map chain from a `MetaDB` `values` resource similarly has the same signature.

For `MetaDB` items, the following initial map filter signature applies:
f((metak, (k, vs))) -> v, ...

Initial map filters for `keys` resources always look like:
f(k) -> v, ...

`DiscoDB` `values` resources initial map filters looke like:
f(v) -> v, ...

In all cases, if the initial filter is also the final filter, `v = k, v`.
The default map filter for most resources is `kvify`, which gives the value an empty key if it is not already a (k, v) pair.

pipelines
---------

/keys
'', k1
'', k2

/values
'', v1
'', v2

/values|kvify|kvswap
v1, ''
v2, ''

/values}count
'', n

/values|kvify|kvswap}count
v1, n1
v2, n2

/indices/[metaindex]/items
metakey, ((key, vs), ...)

"""
from discodb import kvgroup
from itertools import chain

def funcify(maybe_curry):
    if ':' in maybe_curry:
        from functools import partial
        from urllib import unquote
        dotted_name, arg = maybe_curry.split(':', 1)
        return partial(reify(dotted_name), unquote(arg))
    return reify(maybe_curry)

def reify(dotted_name):
    if '.' in dotted_name:
        package, name = dotted_name.rsplit('.', 1)
        return getattr(__import__(package, fromlist=[name]), name)
    return eval(dotted_name)

def iskv(object):
    return isinstance(object, tuple) and len(object) is 2

def filterchain(filters):
    filters = list(filters)
    def f(x):
        seq = x,
        for filter in filters:
            seq = chain(*(filter(x) for x in seq))
        return seq
    return f


def keyify(entry):
    yield entry, ''

def kvify(entry):
    yield entry if iskv(entry) else ('', entry)

def valify(entry):
    yield '', entry

def key((k, v)):
    yield k

def value((k, v)):
    yield v

def kvswap((k, v)):
    yield v, k

def lenv((k, v)):
    yield k, len(v)

def listv((k, v)):
    yield k, list(v)

def hashbin(nbins, entry):
    yield (hash(entry) % eval(nbins)), entry

def where(predicate, entry):
    if iskv(entry):
        k, v = entry
    if eval(predicate):
        yield entry

def evaluate(expression, entry):
    if iskv(entry):
        k, v = entry
    yield eval(expression)

def kvungroup((k, vs)):
    for v in vs:
        yield k, v

def countv((k, vs)):
    yield k, sum(1 for v in vs)

def setv((k, vs)):
    yield k, set(vs)

def sumv((k, vs)):
    yield k, sum(float(v) for v in vs)

def minv((k, vs)):
    yield k, min(vs)

def maxv((k, vs)):
    yield k, max(vs)

def mean((k, vs)):
    total = 0.
    for n, v in enumerate(vs):
        total += float(v)
    yield k, total / n

def count(iter):
    yield sum(1 for x in iter)

def kv_or_v(kviter):
    for k, v in kviter:
        yield (k, v) if k else v
