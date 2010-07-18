import sys
from disco.core import Disco, result_iterator
from disco.settings import DiscoSettings

def map(entry, params):
    for word in entry.split():
        yield word, 1

def reduce(iter, out, params):
    s = {}
    for word, freq in iter:
        s[word] = s.get(word, 0) + int(freq)
    for word, freq in s.iteritems():
        out.add(word, freq)

disco = Disco(DiscoSettings()['DISCO_MASTER'])
print "Starting Disco job.."
print "Go to %s to see status of the job." % disco.master
results = disco.new_job(name="wordcount",
                        input=["http://discoproject.org/media/text/chekhov.txt"],
                        map=map,
                        reduce=reduce,
                        save=True).wait()
print "Job done. Results:"
for word, freq in result_iterator(results):
    print word, freq
