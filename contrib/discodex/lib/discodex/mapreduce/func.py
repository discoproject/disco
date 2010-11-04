from disco.util import iskv, reify as _reify
from itertools import chain

def reify(dotted_name):
    return _reify(dotted_name, globals=globals())

# streams / reduces

def count(iter, *args):
    yield sum(1 for entry in iter)

def count_v(iter, *args):
    for k, vs in iter:
        yield k, sum(1 for v in vs)

def int_vs(iter, *args):
    for k, vs in iter:
        for v in vs:
            yield k, int(v)

def length(iter, *args):
    yield len(iter)

def length_v(iter, *args):
    for k, v in iter:
        yield k, len(v)

def where_v(iter, *args):
    for k, v in iter:
        if v:
            yield k, v

def keys(iter, *args):
    for k, v in iter:
        yield k

def vals(iter, *args):
    for k, v in iter:
        yield v

def head(iter, size, url, params):
    for n, entry in enumerate(iter):
        if n == int(getattr(params, 'n', 10)):
            return
        yield entry

def count_ks(iter, *args):
    from disco.util import kvgroup
    yield sum(1 for kvs in kvgroup(sorted(iter))), None

def count_vs(iter, *args):
    from disco.util import kvgroup
    for k, vs in kvgroup(sorted(iter)):
        yield k, sum(1 for v in vs)

def mean_vs(iter, *args):
    from disco.util import kvgroup
    for k, vs in kvgroup(sorted(iter)):
        total = 0.
        for n, v in enumerate(vs):
            total += float(v)
        yield k, total / n

def sum_vs(iter, *args):
    from disco.util import kvgroup
    for k, vs in kvgroup(sorted(iter)):
        yield k, sum(vs)
