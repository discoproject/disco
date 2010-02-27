# XXX: map_reader / map_input_stream interface changes will affect this code
#
# map_input_stream needs to guarantee iterables for these to work, thus something like:
#
#     def map_input_stream(stream, size, url, params):
#         from disco import func
#         return func.map_line_reader(stream, size, url), size, url
#     map_input_stream = [func.map_input_stream, map_input_stream]

def rawparse(iterable, size, fname):
    for line in iterable:
        for item in line.strip('/').split('/'):
            for kv in item.split(','):
                yield kv.split(':', 1)

def netstrparse(fd, size, fname):
    from disco import func
    return func.netstr_reader(fd, size, fname)

def recordparse(iterable, size, fname):
    from discodex.mapreduce import Record
    for line in iterable:
        yield Record(*line.split())

def csvrecordparse(iterable, size, fname):
    from discodex.mapreduce import Record
    for line in iterable:
        yield Record(*line.split(','))

def enumfieldparse(iterable, size, fname):
    from discodex.mapreduce import Record
    for line in iterable:
        yield Record(**dict((str(n), f) for n, f in enumerate(line.split())))

