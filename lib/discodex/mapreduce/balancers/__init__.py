from .. import module

@module(__package__)
def nchunksbalance(key, nr_reduces, params):
    params.n += 1
    return params.n % nr_reduces
