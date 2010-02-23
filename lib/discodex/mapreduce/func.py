"""
Utility functions used by :mod:`discodex.mapreduce` to construct filter chains plus some builtin filters.

A filter chain is just a sequence of single-argument functions.
Since filter chains are usually specified as a serialized string (i.e. within a URL),
we provide support here for serializing/deserializing the chain.

A single serialized filter is either a fully qualified name, or a name in this modules namespace.
In addition, the filter string can provide an optional string argument,
which will be curried to the function to create a filter.
When a function is specified with optional string argument, the argument is completely url-unquoted.
This is done to support the ReST API, which requires '|' and '}' characters to be url-quoted in filter arguments.

Once constructed, filter chains must adhere to an overall system input/output signature.
This means that constraints are placed on the initial/final functions in a chain,
while filters in the middle of a chain need only adhere to constraints created by surrounding filters.

A filter chain must have one of the following signatures:
        f(entry)   ->     v
        f(entry)   -> (k, v)
        f(entry)   -> (k, v), ...

For a reducefilter chain,
        entry = k, vs

A mapfilter of the form:
        f(entry)   ->     v

will yield from map with '' as the key, e.g.:
/keys
  '', k1
  '', k2
/values
/values|str
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
/values|kvify|kvswap}topk:10

/keys
        map: entry = dbkey
             yield '', dbkey
/values
        map: entry = dbvalue
             yield '', dbvalue

/items
        map: entry = dbkey, dbvalues
             yield dbkey, dbvalues

/query/[query_path]
        map: entry = dbvalue
             yield '', dbvalue

TODO:
/keys/startingwith:host/values|melnorme.parse_time}count
/keys/startingwith:user
  '', u1
  '', u2
/keys/startingwith:user/values
  u1, v1
  u2, v2
/keys/startingwith:user/values|melnorme.parse_time
  u1, t1
  u2, t2
/keys/startingwith:user/values|melnorme.parse_time}min
  u1, d1
  u2, d2

mapfilters:
  gt:X
  gte:X
  lt:X
  lte:X
  numv
  numk
reducefilters:
  count
  minv
  maxv
  top:K     for each key: sorted(vs)[:K] -> yield k, v
  bottom:K
  mean
  median
  std
  unique
"""
from discodb import kvgroup

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

def kvify(entry):
    if iskv(entry):
        return entry
    return '', entry

def kviterify(entry_or_kvseq):
    if hasattr(entry_or_kvseq, '__iter__'):
        if iskv(entry_or_kvseq):
            yield entry_or_kvseq
        else:
            for k, v in entry_or_kvseq:
                yield k, v
    else:
        yield '', entry_or_kvseq

def filterchain(filters):
    filters = list(filters)
    def f(x):
        for filter in filters:
            x = filter(x)
        return x
    return f



def key((k, v)):
    return k

def value((k, v)):
    return v

def kvswap((k, v)):
    return v, k

def lenv((k, v)):
    return k, len(v)

def where(predicate, entry):
    if iskv(entry):
        k, v = entry
    if eval(predicate):
        yield entry

def iwhere(predicate, entryseq):
    for entry in entryseq:
        for e in where(predicate, entry):
            yield e

def evaluate(expression, entry):
    if iskv(entry):
        k, v = entry
    return eval(expression)

def ievaluate(expression, entryseq):
    for entry in entryseq:
        yield evaluate(expression, entry)

def kvungroup((k, vs)):
    for v in vs:
        yield k, v

def count((k, vs)):
    return k, sum(1 for v in vs)

def listv((k, vs)):
    return k, list(vs)

def sumv((k, vs)):
    return k, sum(float(v) for v in vs)

def minv((k, vs)):
    return k, min(vs)

def maxv((k, vs)):
    return k, max(vs)

def mean((k, vs)):
    total = 0.
    for n, v in enumerate(vs):
        total += float(v)
    return k, total / n

def unique((k, vs)):
    return k, set(vs)
