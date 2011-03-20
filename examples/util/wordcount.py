"""
This example could be run and the results printed from the `examples/util` directory in Disco:

disco run wordcount.WordCount http://discoproject.org/media/text/chekhov.txt | xargs disco wait | xargs ddfs xcat
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
