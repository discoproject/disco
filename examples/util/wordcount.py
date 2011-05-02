"""
This example could be run (in this case from the Disco `examples/util` directory):

disco run wordcount.WordCount http://discoproject.org/media/text/chekhov.txt

Assuming this was the last job submitted, the results could be printed:

disco wait @ | xargs ddfs xcat
"""
from disco.core import Job
from disco.util import kvgroup

class WordCount(Job):
    @staticmethod
    def map(line, params):
        for word in line.split():
            yield word, 1

    @staticmethod
    def reduce(iter, params):
        for word, counts in kvgroup(sorted(iter)):
            yield word, sum(counts)
