"""
Usage:
python wordcount_redis.py redis://redis_server:6379:0 redis://redis_server:6379:1

The input is read from db 0 and the output is written to db 1.  The inputs
should be of the form (key, list_of_values) (they are read from the server with the
lrange command.  See the redis documentation for more info).
The output will also be of the form (key, list_of_values).  The reason we use
this approach is to unify the mechanism for the intermediate input-outputs
(which must be (key, list_of_values) with the inputs and outputs).
"""

from disco.schemes.scheme_redis import redis_output_stream
from disco.worker.task_io import task_output_stream
from disco.core import Job, result_iterator

class WordCount(Job):
    reduce_output_stream = (task_output_stream, redis_output_stream)

    @staticmethod
    def map(line, params):
        k, v = line
        yield v, 1

    @staticmethod
    def reduce(iter, params):
        from disco.util import kvgroup
        for word, counts in kvgroup(sorted(iter)):
            yield word, sum(counts)

if __name__ == '__main__':
    import sys
    if len(sys.argv) != 3:
        print "Usage: python wordcount_redis.py <input redis> <output redis>"
        sys.exit(1)

    from wordcount_redis import WordCount
    job = WordCount()
    job.params = {}
    job.params['url'] = sys.argv[2]
    job.run(input=[sys.argv[1]])
    job.wait(show=True)
