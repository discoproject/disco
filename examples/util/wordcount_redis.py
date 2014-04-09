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
