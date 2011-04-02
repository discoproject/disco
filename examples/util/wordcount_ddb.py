"""
This example could be run and the results printed from the `examples/util` directory in Disco:

disco run wordcount_ddb.WordCount http://discoproject.org/media/text/chekhov.txt
"""
from disco.core import Job
from disco.util import kvgroup

from disco.worker.classic.func import discodb_stream

class WordCount(Job):
    reduce_output_stream = discodb_stream

    @staticmethod
    def map(line, params):
        for word in line.split():
            yield word, 1

    @staticmethod
    def reduce(iter, params):
        for word, counts in kvgroup(sorted(iter)):
            yield word, str(sum(counts))
