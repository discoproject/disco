from .. import module

parser = module(__name__)

@parser
def rawparse(iterable, size, fname):
    for line in iterable:
        for item in line.strip('/').split('/'):
            for kv in item.split(','):
                yield kv.split(':', 1)

@parser
def recordparse(iterable, size, fname):
    from discodex.mapreduce import Record
    for line in iterable:
        yield Record(*line.split())

@parser
def enumfieldparse(iterable, size, fname):
    from discodex.mapreduce import Record
    for line in iterable:
        yield Record(**dict((str(n), f) for n, f in enumerate(line.split())))
