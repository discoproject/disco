def parse(iterable, size, fname):
    from discodex.mapreduce import Record
    for item in iterable:
        yield Record(**dict(tuple(x.split(":", 1)) for x in item.split(" ")))

def demux(record, params):
    return zip(record.fieldnames, record.fields)

def balance(key, nr_reduces, params):
    params.n += 1
    return nr_reduces % params.n
