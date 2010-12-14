import sys
from disco.core import Disco, result_iterator
from disco.settings import DiscoSettings

def map(line, params):
    for word in line.split():
        yield word, 1

def reduce(iter, params):
    from disco.util import kvgroup
    for word, counts in kvgroup(sorted(iter)):
        yield word, sum(counts)

disco = Disco(DiscoSettings()['DISCO_MASTER'])
print "Starting Disco job.."
print "Go to %s to see status of the job." % disco.master
results = disco.new_job(name="wordcount",
                        input=["http://discoproject.org/media/text/chekhov.txt"],
                        map=map,
                        reduce=reduce,
                        save=True).wait()
print "Job done. Results:"
for word, count in result_iterator(results):
    print word, count
