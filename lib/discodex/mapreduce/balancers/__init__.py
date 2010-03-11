def nchunksbalance(key, nr_reduces, params):
    from random import randint
    return randint(0, nr_reduces - 1)

def roundrobinbalance(key, nr_reduces, params):
    params.n += 1
    return params.n % nr_reduces
