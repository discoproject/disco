from .. import module

metakeyer = module(__name__)

@metakeyer
def prefixkeyer(key, params):
    for n, letter in enumerate(key):
        yield key[:n + 1], key
