from .. import module

@module(__name__)
def rawparse(iterable, size, fname):
    from discodex.mapreduce import Record
    for line in iterable:
        for item in line.strip('/').split('/'):
            yield Record(**dict(tuple(x.split(":", 1)) for x in item.split(",")))

