from .. import module

@module(__name__)
def nchunksbalance(key, nr_reduces, params):
    params.n += 1
    return params.n % nr_reduces
