"""
This example could be run and the results printed from the `examples/util` directory in Disco:

python query_ddb.py <query> <input> ...
"""
import sys
from disco.core import Job, Disco, result_iterator

from disco.worker.classic.func import nop_map
from disco.schemes.scheme_discodb import input_stream

class Query(Job):
    map_input_stream = (input_stream, )
    map = staticmethod(nop_map)

    @staticmethod
    def map_reader(discodb, size, url, params):
        for k, vs in discodb.metaquery(params):
            yield k, list(vs)

if __name__ == '__main__':
    job = Query().run(input=sys.argv[2:], params=sys.argv[1])
    for k, vs in result_iterator(job.wait()):
        print '%s\t%s' % (k, sum(int(v) for v in vs))
