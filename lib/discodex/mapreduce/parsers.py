def parse(iterable, size, fname):
    from discodex.mapreduce import Record
    for line in iterable:
        for item in line.split('/'):
            yield Record(**dict(tuple(x.split(":", 1)) for x in item.split(",")))

def demux(record, params):
    return zip(record.fieldnames, record.fields)

def balance(key, nr_reduces, params):
    params.n += 1
    return params.n % nr_reduces
